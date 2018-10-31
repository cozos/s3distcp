package com.amazon.elasticmapreduce.s3distcp;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;
import com.google.common.base.Strings;
import emr.hbase.options.OptionWithArg;
import emr.hbase.options.Options;
import emr.hbase.options.SimpleOption;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.regex.Pattern;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.SimpleHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class S3DistCp
  implements Tool
{
  private static final Log LOG = LogFactory.getLog(S3DistCp.class);
  private static final int MAX_LIST_RETRIES = 10;
  public static final String EC2_META_AZ_URL = "http://169.254.169.254/latest/meta-data/placement/availability-zone";
  public static final String S3_ENDPOINT_PDT = "s3-us-gov-west-1.amazonaws.com";
  private static String ec2MetaDataAz = null;
  private Configuration conf;
  
  public static class S3DistCpOptions
  {
    private static final Log LOG = LogFactory.getLog(S3DistCpOptions.class);
    private String srcPath;
    private String dest;
    private boolean numberFiles = false;
    private boolean s3ServerSideEncryption = false;
    private String srcPattern;
    private Long filePerMapper;
    private String groupByPattern;
    private Integer targetSize;
    private boolean appendToLastFile = false;
    private String outputCodec = "keep";
    private String s3Endpoint;
    private boolean deleteOnSuccess = false;
    private boolean disableMultipartUpload = false;
    private boolean requirePreviousManifest = true;
    private String manifestPath;
    private Integer multipartUploadPartSize;
    private Long startingIndex = Long.valueOf(0L);
    private Map<String, ManifestEntry> previousManifest;
    private String previousManifestPath;
    private Boolean copyFromManifest = Boolean.valueOf(false);
    private boolean helpDefined = false;
    private StorageClass storageClass = null;
    private Path srcPrefixesFile = null;
    
    public S3DistCpOptions() {}
    
    public S3DistCpOptions(String[] args, Configuration conf)
      throws IllegalArgumentException, IOException
    {
      Options options = new Options();
      SimpleOption helpOption = options.noArg("--help", "Print help text");
      OptionWithArg srcOption = options.withArg("--src", "Directory to copy files from");
      OptionWithArg destOption = options.withArg("--dest", "Directory to copy files to");
      OptionWithArg srcPatternOption = options.withArg("--srcPattern", "Include only source files matching this pattern");
      OptionWithArg filePerMapperOption = options.withArg("--filesPerMapper", "Place up to this number of files in each map task");
      
      OptionWithArg groupByPatternOption = options.withArg("--groupBy", "Pattern to group input files by");
      OptionWithArg appendToLastFileOption = options.withArg("--appendToLastFile", "Allows appending to the last file that was previously copied when using groupBy");
      
      OptionWithArg targetSizeOption = options.withArg("--targetSize", "Target size for output files");
      OptionWithArg outputCodecOption = options.withArg("--outputCodec", "Compression codec for output files");
      OptionWithArg s3EndpointOption = options.withArg("--s3Endpoint", "S3 endpoint to use for uploading files");
      SimpleOption deleteOnSuccessOption = options.noArg("--deleteOnSuccess", "Delete input files after a successful copy");
      
      SimpleOption disableMultipartUploadOption = options.noArg("--disableMultipartUpload", "Disable the use of multipart upload");
      
      OptionWithArg multipartUploadPartSizeOption = options.withArg("--multipartUploadChunkSize", "The size in MiB of the multipart upload part size");
      
      OptionWithArg startingIndexOption = options.withArg("--startingIndex", "The index to start with for file numbering");
      
      SimpleOption numberFilesOption = options.noArg("--numberFiles", "Prepend sequential numbers the file names");
      SimpleOption s3ServerSideEncryptionOption = options.noArg("--s3ServerSideEncryption", "Copy files to S3 using Amazon S3 Server Side Encryption");
      
      OptionWithArg outputManifest = options.withArg("--outputManifest", "The name of the manifest file");
      OptionWithArg requirePreviousManifestOption = options.withArg("--requirePreviousManifest", "Require that a previous manifest is present if specified");
      
      OptionWithArg previousManifest = options.withArg("--previousManifest", "The path to an existing manifest file");
      SimpleOption copyFromManifest = options.noArg("--copyFromManifest", "Copy from a manifest instead of listing a directory");
      
      OptionWithArg storageClassOption = options.withArg("--storageClass", "STANDARD/STANDARD_IA/REDUCED_REDUNDANCY");
      OptionWithArg srcPrefixesFile = options.withArg("--srcPrefixesFile", "File containing a list of source URI prefixes");
      options.parseArguments(args);
      if (helpOption.defined())
      {
        LOG.info(options.helpText());
        helpDefined = true;
      }
      srcOption.require();
      destOption.require();
      if (srcPrefixesFile.defined()) {
        setSrcPrefixesFile(value);
      }
      if (storageClassOption.defined()) {
        setStorageClass(value);
      }
      if (srcOption.defined()) {
        setSrcPath(value);
      }
      if (destOption.defined()) {
        setDest(value);
      }
      if (numberFilesOption.defined()) {
        setNumberFiles(Boolean.valueOf(value));
      }
      if (s3ServerSideEncryptionOption.defined()) {
        setS3ServerSideEncryption(Boolean.valueOf(value));
      }
      if (srcPatternOption.defined()) {
        setSrcPattern(value);
      }
      if (filePerMapperOption.defined()) {
        setFilePerMapper(value);
      }
      String destScheme = new Path(getDest()).toUri().getScheme();
      if (groupByPatternOption.defined())
      {
        setGroupByPattern(value);
        if (!Utils.isS3Scheme(destScheme)) {
          appendToLastFile = true;
        }
      }
      if (appendToLastFileOption.defined())
      {
        if ((value.toLowerCase().contains("true")) && (Utils.isS3Scheme(destScheme))) {
          throw new IllegalArgumentException("--appendToLastFile option is not supported for s3");
        }
        setAppendToLastFileOption(value);
      }
      if (targetSizeOption.defined()) {
        setTargetSize(value);
      }
      if (outputCodecOption.defined()) {
        setOutputCodec(value);
      }
      if (s3EndpointOption.defined()) {
        setS3Endpoint(value);
      }
      if (deleteOnSuccessOption.defined()) {
        setDeleteOnSuccess(Boolean.valueOf(value));
      }
      if (disableMultipartUploadOption.defined()) {
        setDisableMultipartUpload(Boolean.valueOf(value));
      }
      if (multipartUploadPartSizeOption.defined()) {
        setMultipartUploadPartSize(value);
      }
      if (startingIndexOption.defined()) {
        setStartingIndex(value);
      }
      if (numberFilesOption.defined()) {
        setNumberFiles(Boolean.valueOf(value));
      }
      if (outputManifest.defined()) {
        setManifestPath(value);
      }
      if (requirePreviousManifestOption.defined()) {
        setRequirePreviousManifest(value);
      }
      if (previousManifest.defined()) {
        previousManifestPath = value;
      }
      if (copyFromManifest.defined()) {
        setCopyFromManifest(true);
      }
      if ((previousManifest.defined()) && (!copyFromManifest.defined())) {
        try
        {
          setPreviousManifest(loadManifest(new Path(value), conf));
        }
        catch (RuntimeException e)
        {
          if (requirePreviousManifest) {
            throw e;
          }
        }
      }
    }
    
    public Path getSrcPrefixesFile()
    {
      return srcPrefixesFile;
    }
    
    public void setSrcPrefixesFile(String path)
    {
      srcPrefixesFile = new Path(path);
    }
    
    public StorageClass getStorageClass()
    {
      return storageClass;
    }
    
    public void setStorageClass(String storageClass)
    {
      if (Strings.isNullOrEmpty(storageClass)) {
        this.storageClass = null;
      }
      this.storageClass = StorageClass.fromValue(storageClass);
    }
    
    public String getSrcPath()
    {
      return srcPath;
    }
    
    public void setSrcPath(String srcPath)
    {
      this.srcPath = srcPath;
    }
    
    public String getDest()
    {
      return dest;
    }
    
    public void setDest(String dest)
    {
      this.dest = dest;
    }
    
    public Boolean getNumberFiles()
    {
      return Boolean.valueOf(numberFiles);
    }
    
    public void setNumberFiles(Boolean numberFiles)
    {
      this.numberFiles = numberFiles.booleanValue();
    }
    
    public Boolean getS3ServerSideEncryption()
    {
      return Boolean.valueOf(s3ServerSideEncryption);
    }
    
    public void setS3ServerSideEncryption(Boolean s3ServerSideEncryption)
    {
      this.s3ServerSideEncryption = s3ServerSideEncryption.booleanValue();
    }
    
    public String getSrcPattern()
    {
      return srcPattern;
    }
    
    public void setSrcPattern(String srcPattern)
    {
      this.srcPattern = srcPattern;
    }
    
    public Long getFilePerMapper()
    {
      return filePerMapper;
    }
    
    public void setFilePerMapper(String filePerMapper)
    {
      this.filePerMapper = toLong(filePerMapper);
    }
    
    private Long toLong(String s)
    {
      if (s != null) {
        return Long.valueOf(s);
      }
      return null;
    }
    
    private Integer toInteger(String s)
    {
      if (s != null) {
        return Integer.valueOf(s);
      }
      return null;
    }
    
    public String getGroupByPattern()
    {
      return groupByPattern;
    }
    
    public void setGroupByPattern(String groupByPattern)
    {
      this.groupByPattern = groupByPattern;
    }
    
    public Integer getTargetSize()
    {
      return targetSize;
    }
    
    public void setTargetSize(String targetSize)
    {
      this.targetSize = toInteger(targetSize);
    }
    
    public boolean getAppendToLastFileOption()
    {
      return appendToLastFile;
    }
    
    public void setAppendToLastFileOption(String appendToLastFile)
    {
      this.appendToLastFile = Boolean.valueOf(appendToLastFile).booleanValue();
    }
    
    public String getOutputCodec()
    {
      return outputCodec;
    }
    
    public void setOutputCodec(String outputCodec)
    {
      this.outputCodec = outputCodec;
    }
    
    public String getS3Endpoint()
    {
      return s3Endpoint;
    }
    
    public void setS3Endpoint(String s3Endpoint)
    {
      this.s3Endpoint = s3Endpoint;
    }
    
    public Boolean getDeleteOnSuccess()
    {
      return Boolean.valueOf(deleteOnSuccess);
    }
    
    public void setDeleteOnSuccess(Boolean deleteOnSuccess)
    {
      this.deleteOnSuccess = deleteOnSuccess.booleanValue();
    }
    
    public Boolean getDisableMultipartUpload()
    {
      return Boolean.valueOf(disableMultipartUpload);
    }
    
    public void setDisableMultipartUpload(Boolean disableMultipartUpload)
    {
      this.disableMultipartUpload = disableMultipartUpload.booleanValue();
    }
    
    public void setRequirePreviousManifest(String requirePreviousManifest)
    {
      this.requirePreviousManifest = Boolean.valueOf(requirePreviousManifest).booleanValue();
    }
    
    public String getManifestPath()
    {
      return manifestPath;
    }
    
    public void setManifestPath(String manifestPath)
    {
      this.manifestPath = manifestPath;
    }
    
    public Integer getMultipartUploadPartSize()
    {
      return multipartUploadPartSize;
    }
    
    public void setMultipartUploadPartSize(String multipartUploadPartSize)
    {
      this.multipartUploadPartSize = toInteger(multipartUploadPartSize);
    }
    
    public Long getStartingIndex()
    {
      return startingIndex;
    }
    
    public void setStartingIndex(String startingIndex)
    {
      if (startingIndex != null) {
        this.startingIndex = Long.valueOf(startingIndex);
      } else {
        this.startingIndex = Long.valueOf(0L);
      }
    }
    
    public String getPreviousManifestPath()
    {
      return previousManifestPath;
    }
    
    public boolean isCopyFromManifest()
    {
      return copyFromManifest.booleanValue();
    }
    
    public void setCopyFromManifest(boolean copyFromManifest)
    {
      this.copyFromManifest = Boolean.valueOf(copyFromManifest);
    }
    
    public Map<String, ManifestEntry> getPreviousManifest()
    {
      return previousManifest;
    }
    
    public void setPreviousManifest(Map<String, ManifestEntry> previousManifest)
    {
      this.previousManifest = previousManifest;
    }
    
    public boolean isHelpDefined()
    {
      return helpDefined;
    }
    
    public static Map<String, ManifestEntry> loadManifest(Path manifestPath, Configuration conf)
    {
      Map<String, ManifestEntry> manifest = new TreeMap();
      try
      {
        ManifestIterator manifestIterator = new ManifestIterator(manifestPath, conf);Throwable localThrowable2 = null;
        try
        {
          while (manifestIterator.hasNext())
          {
            ManifestEntry entry = manifestIterator.getNext();
            manifest.put(baseName, entry);
          }
        }
        catch (Throwable localThrowable1)
        {
          localThrowable2 = localThrowable1;throw localThrowable1;
        }
        finally
        {
          if (manifestIterator != null) {
            if (localThrowable2 != null) {
              try
              {
                manifestIterator.close();
              }
              catch (Throwable x2)
              {
                localThrowable2.addSuppressed(x2);
              }
            } else {
              manifestIterator.close();
            }
          }
        }
      }
      catch (Exception e)
      {
        throw new RuntimeException("Failed to load manifest file '" + manifestPath + "'", e);
      }
      return manifest;
    }
  }
  
  private List<Pair<Path, Boolean>> getSrcPrefixes(Path srcPath, boolean srcEndWithSlash, Path srcPrefixesFile)
  {
    List<Pair<Path, Boolean>> srcPrefixes = new ArrayList();
    if (srcPrefixesFile == null)
    {
      srcPrefixes.add(new Pair(srcPath, Boolean.valueOf(srcEndWithSlash)));
      return srcPrefixes;
    }
    try
    {
      FileSystem fs = srcPrefixesFile.getFileSystem(conf);
      InputStream is = fs.open(srcPrefixesFile);Throwable localThrowable2 = null;
      try
      {
        String srcPathStr = srcPath.toString();
        if ((srcEndWithSlash) && (!srcPathStr.endsWith("/"))) {
          srcPathStr = srcPathStr + "/";
        }
        Scanner scanner = new Scanner(is);
        while (scanner.hasNextLine())
        {
          String prefix = scanner.nextLine();
          if (!prefix.startsWith(srcPathStr))
          {
            LOG.fatal("srcPrefix: " + prefix + " does not start with src: " + srcPathStr);
            System.exit(3);
          }
          else
          {
            srcPrefixes.add(new Pair(new Path(prefix), Boolean.valueOf(prefix.endsWith("/"))));
          }
        }
      }
      catch (Throwable localThrowable1)
      {
        localThrowable2 = localThrowable1;throw localThrowable1;
      }
      finally
      {
        if (is != null) {
          if (localThrowable2 != null) {
            try
            {
              is.close();
            }
            catch (Throwable x2)
            {
              localThrowable2.addSuppressed(x2);
            }
          } else {
            is.close();
          }
        }
      }
    }
    catch (IOException e)
    {
      LOG.fatal("Failed to read srcPrefixesFile: " + srcPrefixesFile, e);
      System.exit(3);
    }
    if (srcPrefixes.size() == 0) {
      srcPrefixes.add(new Pair(srcPath, Boolean.valueOf(srcEndWithSlash)));
    }
    return srcPrefixes;
  }
  
  public void createInputFileList(Configuration conf, Path srcPath, FileInfoListing fileInfoListing, boolean srcEndWithSlash)
  {
    createInputFileList(conf, srcPath, null, fileInfoListing, srcEndWithSlash);
  }
  
  private void createInputFileList(Configuration conf, Path srcPath, Path srcPrefixesFile, FileInfoListing fileInfoListing, boolean srcEndWithSlash)
  {
    URI srcUri = srcPath.toUri();
    if (Utils.isS3Scheme(srcUri.getScheme()))
    {
      List<Pair<Path, Boolean>> srcPrefixes = getSrcPrefixes(srcPath, srcEndWithSlash, srcPrefixesFile);
      if (srcPrefixes.size() == 1) {
        createInputFileListS3(conf, ((Path)((Pair)srcPrefixes.get(0)).getFirst()).toUri(), fileInfoListing, ((Boolean)((Pair)srcPrefixes.get(0)).getSecond()).booleanValue());
      } else {
        parallelizedCreateInputFileListS3(conf, srcPrefixes, fileInfoListing);
      }
    }
    else
    {
      try
      {
        FileSystem fs = srcPath.getFileSystem(conf);
        Queue<Path> pathsToVisit = new ArrayDeque();
        for (Pair<Path, Boolean> prefix : getSrcPrefixes(srcPath, srcEndWithSlash, srcPrefixesFile)) {
          pathsToVisit.add(prefix.getFirst());
        }
        while (pathsToVisit.size() > 0)
        {
          Path curPath = (Path)pathsToVisit.remove();
          FileStatus[] statuses = fs.listStatus(curPath);
          for (FileStatus status : statuses) {
            if (status.isDir()) {
              pathsToVisit.add(status.getPath());
            } else {
              fileInfoListing.add(status.getPath(), status.getLen());
            }
          }
        }
      }
      catch (IOException e)
      {
        LOG.fatal("Failed to list input files", e);
        System.exit(-4);
      }
    }
  }
  
  private void parallelizedCreateInputFileListS3(final Configuration conf, List<Pair<Path, Boolean>> srcPrefixes, final FileInfoListing fileInfoListing)
  {
    int numThreads = conf.getInt("s3DistCp.listfiles.srcPrefixes.numThreads", 20);
    ThreadPoolExecutor listExecutor = Utils.createExecutorService("s3distcp-list-worker", numThreads);
    List<Future<Void>> futures = new ArrayList();
    for (final Pair<Path, Boolean> prefix : srcPrefixes)
    {
      Future<Void> future = listExecutor.submit(new Callable()
      {
        public Void call()
          throws Exception
        {
          createInputFileListS3(conf, ((Path)prefix.getFirst()).toUri(), fileInfoListing, ((Boolean)prefix.getSecond()).booleanValue());
          return null;
        }
      });
      futures.add(future);
    }
    try
    {
      for (Future future : futures) {
        future.get();
      }
      return;
    }
    catch (InterruptedException|ExecutionException e)
    {
      LOG.fatal("Failed to list objects", e);
      throw new RuntimeException(e);
    }
    finally
    {
      try
      {
        listExecutor.shutdown();
      }
      catch (Exception e)
      {
        LOG.warn("Error shutdown executor", e);
      }
    }
  }
  
  public void createInputFileListS3(Configuration conf, URI srcUri, FileInfoListing fileInfoListing, boolean srcEndWithSlash)
  {
    AmazonS3Client s3Client = createAmazonS3Client(conf);
    ObjectListing objects = null;
    boolean finished = false;
    int retryCount = 0;
    String scheme = srcUri.getScheme() + "://";
    while (!finished)
    {
      ListObjectsRequest listObjectRequest = new ListObjectsRequest().withBucketName(Utils.uriToBucket(srcUri));
      if (srcUri.getPath().length() > 1)
      {
        String prefix = srcUri.getPath().substring(1);
        if ((srcEndWithSlash) && (!prefix.endsWith("/"))) {
          prefix = prefix + "/";
        }
        listObjectRequest.setPrefix(prefix);
      }
      if (objects != null) {
        listObjectRequest.withMaxKeys(Integer.valueOf(1000)).withMarker(objects.getNextMarker());
      }
      try
      {
        objects = s3Client.listObjects(listObjectRequest);
        retryCount = 0;
      }
      catch (AmazonClientException e)
      {
        retryCount++;
        if (retryCount > 10)
        {
          LOG.fatal("Failed to list objects", e);
          throw e;
        }
        LOG.warn("Error listing objects: " + e.getMessage(), e);
      }
      continue;
      
      List<Pair<Path, Long>> s3Files = new ArrayList();
      for (S3ObjectSummary object : objects.getObjectSummaries()) {
        if (object.getKey().endsWith("/"))
        {
          LOG.info("Skipping key '" + object.getKey() + "' because it ends with '/'");
        }
        else
        {
          String s3FilePath = scheme + object.getBucketName() + "/" + object.getKey();
          s3Files.add(new Pair(new Path(s3FilePath), Long.valueOf(object.getSize())));
        }
      }
      synchronized (this)
      {
        for (Pair<Path, Long> s3File : s3Files)
        {
          LOG.debug("About to add " + s3File.getFirst());
          fileInfoListing.add((Path)s3File.getFirst(), ((Long)s3File.getSecond()).longValue());
        }
      }
      if (!objects.isTruncated()) {
        finished = true;
      }
    }
  }
  
  public static AmazonS3Client createAmazonS3Client(Configuration conf)
  {
    String accessKeyId = conf.get("fs.s3n.awsAccessKeyId");
    String SecretAccessKey = conf.get("fs.s3n.awsSecretAccessKey");
    AmazonS3Client s3Client;
    if ((accessKeyId != null) && (SecretAccessKey != null))
    {
      AmazonS3Client s3Client = new AmazonS3Client(new BasicAWSCredentials(accessKeyId, SecretAccessKey));
      LOG.info("Hadoop Configuration is used to create AmazonS3Client. KeyId: " + accessKeyId);
    }
    else
    {
      AWSCredentialsProvider provider = new DefaultAWSCredentialsProviderChain();
      s3Client = new AmazonS3Client(provider);
      LOG.info("DefaultAWSCredentialsProviderChain is used to create AmazonS3Client. KeyId: " + provider.getCredentials().getAWSAccessKeyId());
    }
    String endpoint = conf.get("fs.s3n.endpoint");
    if ((endpoint == null) && (isGovCloud())) {
      endpoint = "s3-us-gov-west-1.amazonaws.com";
    }
    if (endpoint != null)
    {
      LOG.info("AmazonS3Client setEndpoint " + endpoint);
      s3Client.setEndpoint(endpoint);
    }
    return s3Client;
  }
  
  private static String getHostName()
  {
    try
    {
      InetAddress addr = InetAddress.getLocalHost();
      return addr.getHostName();
    }
    catch (UnknownHostException ex) {}
    return "unknown";
  }
  
  private static boolean isGovCloud()
  {
    if (ec2MetaDataAz != null) {
      return ec2MetaDataAz.startsWith("us-gov-west-1");
    }
    String hostname = getHostName();
    int timeout = hostname.startsWith("ip-") ? 30000 : 1000;
    GetMethod getMethod = new GetMethod("http://169.254.169.254/latest/meta-data/placement/availability-zone");
    try
    {
      HttpConnectionManager manager = new SimpleHttpConnectionManager();
      HttpConnectionManagerParams params = manager.getParams();
      
      params.setConnectionTimeout(timeout);
      
      params.setSoTimeout(timeout);
      HttpClient httpClient = new HttpClient(manager);
      int status = httpClient.executeMethod(getMethod);
      if ((status < 200) || (status > 299))
      {
        LOG.info("error status code" + status + " GET " + "http://169.254.169.254/latest/meta-data/placement/availability-zone");
      }
      else
      {
        ec2MetaDataAz = getMethod.getResponseBodyAsString().trim();
        LOG.info("GET http://169.254.169.254/latest/meta-data/placement/availability-zone result: " + ec2MetaDataAz);
        return ec2MetaDataAz.startsWith("us-gov-west-1");
      }
    }
    catch (Exception e)
    {
      LOG.info("GET http://169.254.169.254/latest/meta-data/placement/availability-zone exception: " + e.getMessage());
    }
    finally
    {
      getMethod.releaseConnection();
    }
    return false;
  }
  
  public int run(String[] args)
    throws IllegalArgumentException, IOException
  {
    if ((args != null) && (args.length > 0))
    {
      StringBuilder sb = new StringBuilder();
      for (String arg : args) {
        sb.append(arg).append(" ");
      }
      LOG.info("S3DistCp args: " + sb.toString());
    }
    S3DistCpOptions options = new S3DistCpOptions(args, conf);
    if (options.isHelpDefined()) {
      return 0;
    }
    return run(options);
  }
  
  public int run(S3DistCpOptions options)
  {
    Configuration jobConf = getConf();
    
    Path srcPath = new Path(options.getSrcPath());
    if (!srcPath.isAbsolute())
    {
      LOG.fatal("Source path must be absolute");
      System.exit(5);
    }
    String tempDirRoot = "hdfs:///tmp/" + UUID.randomUUID();
    Path outputPath = new Path(tempDirRoot, "output");
    Path inputPath = new Path(tempDirRoot, "files");
    Path tempPath = new Path(tempDirRoot, "tempspace");
    Path destPath = new Path(options.getDest());
    if (!destPath.isAbsolute())
    {
      LOG.fatal("Destination path must be absolute");
      System.exit(4);
    }
    LOG.info("Using output path '" + outputPath.toString() + "'");
    
    jobConf.set("s3DistCp.copyfiles.destDir", destPath.toString());
    jobConf.setBoolean("s3DistCp.copyfiles.reducer.numberFiles", options.getNumberFiles().booleanValue());
    if (options.getS3ServerSideEncryption() != null) {
      jobConf.setBoolean("fs.s3.enableServerSideEncryption", options.getS3ServerSideEncryption().booleanValue());
    }
    File manifestFile = null;
    if (options.getManifestPath() != null) {
      manifestFile = new File(options.getManifestPath());
    }
    jobConf.setBoolean("s3distcp.copyFiles.appendToLastFile", options.getAppendToLastFileOption());
    if (options.getS3Endpoint() != null) {
      jobConf.set("fs.s3n.endpoint", options.getS3Endpoint());
    } else if (isGovCloud()) {
      jobConf.set("fs.s3n.endpoint", "s3-us-gov-west-1.amazonaws.com");
    }
    if (options.getDisableMultipartUpload() != null) {
      jobConf.setBoolean("fs.s3n.multipart.uploads.enabled", !options.getDisableMultipartUpload().booleanValue());
    }
    if (options.getMultipartUploadPartSize() != null)
    {
      int partSize = options.getMultipartUploadPartSize().intValue();
      if (partSize < 5)
      {
        LOG.fatal("Multipart upload part size of " + partSize + " MiB is too small.");
        return 2;
      }
      if (partSize > 5120)
      {
        LOG.fatal("Multipart upload part size of " + partSize + " MiB is too large.");
        return 2;
      }
      jobConf.setInt("fs.s3n.multipart.uploads.split.size", options.getMultipartUploadPartSize().intValue() * 1024 * 1024);
    }
    if (options.getTargetSize() != null) {
      try
      {
        long targetSize = options.getTargetSize().intValue();
        jobConf.setLong("s3DistCp.copyfiles.reducer.targetSize", targetSize * 1024L * 1024L);
      }
      catch (Exception e)
      {
        System.err.println("Error parsing target file size");
        return 2;
      }
    }
    String outputCodec = options.getOutputCodec();
    LOG.debug("outputCodec: " + outputCodec);
    jobConf.set("s3DistCp.copyfiles.reducer.outputCodec", outputCodec);
    jobConf.setBoolean("s3DistCp.copyFiles.deleteFilesOnSuccess", options.getDeleteOnSuccess().booleanValue());
    if (options.getStorageClass() != null) {
      jobConf.set("fs.s3.storageClass", options.getStorageClass().toString());
    }
    deleteRecursive(jobConf, inputPath);
    deleteRecursive(jobConf, outputPath);
    try
    {
      FileSystem fs = FileSystem.get(srcPath.toUri(), jobConf);
      srcPath = fs.getFileStatus(srcPath).getPath();
    }
    catch (Exception e)
    {
      LOG.fatal("Failed to get source file system", e);
      throw new RuntimeException("Failed to get source file system", e);
    }
    jobConf.set("s3DistCp.copyfiles.srcDir", srcPath.toString());
    
    FileInfoListing fileInfoListing = null;
    try
    {
      Map<String, ManifestEntry> previousManifest = null;
      if (!copyFromManifest.booleanValue()) {
        previousManifest = options.getPreviousManifest();
      }
      fileInfoListing = new FileInfoListing(jobConf, srcPath, inputPath, destPath, options.getStartingIndex().longValue(), manifestFile, previousManifest);
    }
    catch (IOException e1)
    {
      LOG.fatal("Error initializing manifest file", e1);
      System.exit(5);
    }
    if (options.getSrcPattern() != null) {
      fileInfoListing.setSrcPattern(Pattern.compile(options.getSrcPattern()));
    }
    if (options.getGroupByPattern() != null)
    {
      String groupByPattern = options.getGroupByPattern();
      if ((!groupByPattern.contains("(")) || (!groupByPattern.contains(")")))
      {
        LOG.fatal("Group by pattern must contain at least one group.  Use () to enclose a group");
        System.exit(1);
      }
      try
      {
        fileInfoListing.setGroupBy(Pattern.compile(groupByPattern));
        jobConf.set("s3DistCp.listfiles.groupByPattern", groupByPattern);
      }
      catch (Exception e)
      {
        System.err.println("Invalid group by pattern");
        System.exit(1);
      }
    }
    if (options.getFilePerMapper() != null) {
      fileInfoListing.setRecordsPerFile(options.getFilePerMapper());
    }
    try
    {
      if ((options.isCopyFromManifest()) && ((options.getPreviousManifest() != null) || (options.getPreviousManifestPath() != null)))
      {
        if (options.getPreviousManifest() != null) {
          fileInfoListing = addFilesFromLoadedPrevManifest(options.getPreviousManifest(), fileInfoListing);
        } else {
          fileInfoListing = addFilesFromPrevManifest(new Path(options.getPreviousManifestPath()), jobConf, fileInfoListing);
        }
      }
      else
      {
        boolean srcEndWithSlash = (options.getSrcPath() != null) && (options.getSrcPath().endsWith("/"));
        createInputFileList(jobConf, srcPath, options.getSrcPrefixesFile(), fileInfoListing, srcEndWithSlash);
      }
      LOG.info("Created " + fileInfoListing.getFileIndex() + " files to copy " + fileInfoListing.getRecordIndex() + " files ");
    }
    finally
    {
      fileInfoListing.close();
    }
    try
    {
      FileSystem fs = FileSystem.get(destPath.toUri(), jobConf);
      fs.mkdirs(destPath);
    }
    catch (IOException e)
    {
      LOG.fatal("Failed to create destination path " + destPath, e);
      return 7;
    }
    try
    {
      Job job = Job.getInstance(jobConf);
      job.setJarByClass(getClass());
      job.setJobName("S3DistCp: " + srcPath.toString() + " -> " + destPath.toString());
      
      job.setReduceSpeculativeExecution(false);
      FileInputFormat.addInputPath(job, inputPath);
      FileOutputFormat.setOutputPath(job, outputPath);
      
      job.setInputFormatClass(SequenceFileInputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(FileInfo.class);
      job.setMapperClass(GroupFilesMapper.class);
      job.setReducerClass(CopyFilesReducer.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      int reducerNum = jobConf.getInt("mapreduce.job.reduces", 10);
      if (reducerNum <= 1) {
        reducerNum = 10;
      }
      LOG.info("Reducer number: " + reducerNum);
      job.setNumReduceTasks(reducerNum);
      job.submit();
      
      boolean jobSucceeded = job.waitForCompletion(true);
      
      deleteRecursiveNoThrow(jobConf, tempPath);
      
      long reduceOutputRecords = job.getCounters().findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS).getValue();
      if (reduceOutputRecords > 0L)
      {
        LOG.error(reduceOutputRecords + " files failed to copy");
        throw new RuntimeException(reduceOutputRecords + " files failed to copy");
      }
      FileSystem tempFs = FileSystem.get(tempPath.toUri(), jobConf);
      tempFs.delete(tempPath, true);
      if (!jobSucceeded)
      {
        String errorMessage = "The MapReduce job failed: " + job.getStatus().getFailureInfo();
        LOG.error(errorMessage);
        throw new RuntimeException(errorMessage);
      }
      if (manifestFile != null)
      {
        FileSystem destFs = FileSystem.get(destPath.toUri(), jobConf);
        destFs.copyFromLocalFile(new Path(manifestFile.getAbsolutePath()), destPath);
        manifestFile.delete();
      }
    }
    catch (IOException e)
    {
      deleteRecursiveNoThrow(jobConf, tempPath);
      throw new RuntimeException("Error running job", e);
    }
    catch (InterruptedException e)
    {
      deleteRecursiveNoThrow(jobConf, tempPath);
      LOG.warn(e.getMessage(), e);
    }
    catch (ClassNotFoundException e)
    {
      LOG.warn(e.getMessage(), e);
    }
    return 0;
  }
  
  private FileInfoListing addFilesFromLoadedPrevManifest(Map<String, ManifestEntry> previousManifest, FileInfoListing fileInfoListing)
  {
    for (ManifestEntry entry : previousManifest.values()) {
      fileInfoListing.add(new Path(path), new Path(srcDir), size);
    }
    return fileInfoListing;
  }
  
  private FileInfoListing addFilesFromPrevManifest(Path manifestPath, Configuration config, FileInfoListing fileInfoListing)
  {
    try
    {
      ManifestIterator manifestIterator = new ManifestIterator(manifestPath, conf);Throwable localThrowable2 = null;
      try
      {
        while (manifestIterator.hasNext())
        {
          ManifestEntry entry = manifestIterator.getNext();
          fileInfoListing.add(new Path(path), new Path(srcDir), size);
        }
      }
      catch (Throwable localThrowable1)
      {
        localThrowable2 = localThrowable1;throw localThrowable1;
      }
      finally
      {
        if (manifestIterator != null) {
          if (localThrowable2 != null) {
            try
            {
              manifestIterator.close();
            }
            catch (Throwable x2)
            {
              localThrowable2.addSuppressed(x2);
            }
          } else {
            manifestIterator.close();
          }
        }
      }
    }
    catch (Exception e)
    {
      throw new RuntimeException("Failed to add file info from previous manifest file '" + manifestPath + "'", e);
    }
    return fileInfoListing;
  }
  
  private void deleteRecursiveNoThrow(Configuration conf, Path path)
  {
    LOG.info("Try to recursively delete " + path.toString());
    try
    {
      FileSystem.get(path.toUri(), conf).delete(path, true);
    }
    catch (IOException e)
    {
      LOG.info("Failed to recursively delete " + path.toString());
    }
  }
  
  private void deleteRecursive(Configuration conf, Path outputPath)
  {
    try
    {
      FileSystem.get(outputPath.toUri(), conf).delete(outputPath, true);
    }
    catch (IOException e)
    {
      throw new RuntimeException("Unable to delete directory " + outputPath.toString(), e);
    }
  }
  
  public Configuration getConf()
  {
    return conf;
  }
  
  public void setConf(Configuration conf)
  {
    this.conf = conf;
  }
}

/* Location:
 * Qualified Name:     com.amazon.elasticmapreduce.s3distcp.S3DistCp
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */