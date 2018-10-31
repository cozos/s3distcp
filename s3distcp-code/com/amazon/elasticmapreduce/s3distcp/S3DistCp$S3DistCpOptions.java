package com.amazon.elasticmapreduce.s3distcp;

import com.amazonaws.services.s3.model.StorageClass;
import com.google.common.base.Strings;
import emr.hbase.options.OptionWithArg;
import emr.hbase.options.Options;
import emr.hbase.options.SimpleOption;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class S3DistCp$S3DistCpOptions
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
  
  public S3DistCp$S3DistCpOptions() {}
  
  public S3DistCp$S3DistCpOptions(String[] args, Configuration conf)
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

/* Location:
 * Qualified Name:     com.amazon.elasticmapreduce.s3distcp.S3DistCp.S3DistCpOptions
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */