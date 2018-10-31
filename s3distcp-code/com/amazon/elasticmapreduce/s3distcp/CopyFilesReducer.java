package com.amazon.elasticmapreduce.s3distcp;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.hadoop.compression.lzo.LzopCodec;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.Progressable;

public class CopyFilesReducer
  extends Reducer<Text, FileInfo, Text, Text>
{
  private static final Log LOG = LogFactory.getLog(CopyFilesReducer.class);
  private static final List<String> validCodecs = Lists.newArrayList(new String[] { "snappy", "gz", "lzo", "lzop", "gzip" });
  private Reducer<Text, FileInfo, Text, Text>.Context context;
  private SimpleExecutor transferQueue;
  private Set<FileInfo> uncommittedFiles;
  private long targetSize;
  private int bufferSize;
  private int numTransferRetries;
  private String outputCodec;
  private String outputSuffix;
  private boolean deleteOnSuccess;
  private boolean numberFiles;
  private boolean appendToLastFile;
  private boolean group;
  private Configuration conf;
  
  protected void cleanup(Reducer<Text, FileInfo, Text, Text>.Context context)
    throws IOException, InterruptedException
  {
    transferQueue.close();
    synchronized (uncommittedFiles)
    {
      if (uncommittedFiles.size() > 0)
      {
        LOG.warn("CopyFilesReducer uncommitted files " + uncommittedFiles.size());
        for (FileInfo fileInfo : uncommittedFiles)
        {
          LOG.warn("Failed to upload " + inputFileName);
          context.write(outputFileName, inputFileName);
        }
        String message = String.format("Reducer task failed to copy %d files: %s etc", new Object[] { Integer.valueOf(uncommittedFiles.size()), uncommittedFiles.iterator().next()).inputFileName });
        
        throw new RuntimeException(message);
      }
    }
  }
  
  public Configuration getConf()
  {
    return conf;
  }
  
  public boolean shouldDeleteOnSuccess()
  {
    return deleteOnSuccess;
  }
  
  protected void setup(Reducer<Text, FileInfo, Text, Text>.Context context)
    throws IOException, InterruptedException
  {
    conf = context.getConfiguration();
    int queueSize = conf.getInt("s3DistCp.copyfiles.mapper.queueSize", 10);
    int numWorkers = conf.getInt("s3DistCp.copyfiles.mapper.numWorkers", 5);
    bufferSize = conf.getInt("s3DistCp.copyfiles.mapper.bufferSize", 1048576);
    targetSize = conf.getLong("s3DistCp.copyfiles.reducer.targetSize", Long.MAX_VALUE);
    outputCodec = conf.get("s3DistCp.copyfiles.reducer.outputCodec").toLowerCase();
    numberFiles = conf.getBoolean("s3DistCp.copyfiles.reducer.numberFiles", false);
    transferQueue = new SimpleExecutor(queueSize, numWorkers);
    uncommittedFiles = Collections.synchronizedSet(new HashSet());
    deleteOnSuccess = conf.getBoolean("s3DistCp.copyFiles.deleteFilesOnSuccess", false);
    numTransferRetries = conf.getInt("s3DistCp.copyfiles.mapper.numRetries", 10);
    appendToLastFile = conf.getBoolean("s3distcp.copyFiles.appendToLastFile", false);
    group = (conf.get("s3DistCp.listfiles.groupByPattern", null) != null);
  }
  
  public int getNumTransferRetries()
  {
    return numTransferRetries;
  }
  
  public int getBufferSize()
  {
    return bufferSize;
  }
  
  private String makeFinalPath(long fileUid, String finalDir, String groupId, String groupIndex)
  {
    String[] groupIds = groupId.split("/");
    groupId = groupIds[(groupIds.length - 1)];
    if (numberFiles) {
      groupId = fileUid + groupId;
    }
    String suffix;
    String suffix;
    if (!Strings.isNullOrEmpty(outputSuffix)) {
      suffix = groupIndex + "." + outputSuffix;
    } else {
      suffix = groupIndex;
    }
    String groupIdSuffix = Utils.getSuffix(groupId);
    if ((!Strings.isNullOrEmpty(groupIdSuffix)) && (validCodecs.contains(groupIdSuffix))) {
      return finalDir + "/" + Utils.replaceSuffix(groupId, suffix);
    }
    return finalDir + "/" + Utils.appendSuffix(groupId, suffix);
  }
  
  private String determineOutputSuffix(List<FileInfo> fileInfos)
  {
    if (outputCodec.equalsIgnoreCase("keep"))
    {
      String codec = null;
      for (??? = fileInfos.iterator(); ???.hasNext();)
      {
        ??? = (FileInfo)???.next();
        String currentSuffix = Utils.getSuffix(inputFileName.toString()).toLowerCase();
        if (!validCodecs.contains(currentSuffix)) {
          currentSuffix = "";
        }
        if (codec == null) {
          codec = currentSuffix;
        } else if (!codec.equals(currentSuffix)) {
          throw new RuntimeException("Cannot keep compression scheme for input files with different compression schemes.");
        }
      }
      if (codec == null) {
        return null;
      }
      switch (codec)
      {
      case "gz": 
      case "gzip": 
        return "gz";
      case "lzo": 
      case "lzop": 
        return "lzo";
      case "snappy": 
        return "snappy";
      case "": 
        return "";
      }
      throw new RuntimeException("Unsupported output codec: " + codec);
    }
    if (outputCodec.equalsIgnoreCase("none"))
    {
      String suffix = null;
      for (FileInfo fileInfo : fileInfos)
      {
        String currentSuffix = Utils.getSuffix(inputFileName.toString()).toLowerCase();
        if (suffix == null) {
          suffix = currentSuffix;
        } else if (!suffix.equals(currentSuffix)) {
          return "";
        }
      }
      if (validCodecs.contains(suffix)) {
        suffix = "";
      }
      return suffix;
    }
    return outputCodec.toLowerCase();
  }
  
  protected void reduce(Text groupKey, Iterable<FileInfo> fileInfos, Reducer<Text, FileInfo, Text, Text>.Context context)
    throws IOException, InterruptedException
  {
    this.context = context;
    long curSize = 0L;
    int groupNum = -1;
    List<FileInfo> allFiles = new ArrayList();
    List<FileInfo> curFiles = new ArrayList();
    String groupId = Utils.cleanupColonsAndSlashes(groupKey.toString());
    Path finalPath = null;
    while (fileInfos.iterator().hasNext()) {
      allFiles.add(((FileInfo)fileInfos.iterator().next()).clone());
    }
    outputSuffix = determineOutputSuffix(allFiles);
    LOG.info("Output suffix: '" + outputSuffix + "'");
    for (int i = 0; i < allFiles.size(); i++)
    {
      FileInfo fileInfo = ((FileInfo)allFiles.get(i)).clone();
      curSize += fileSize.get();
      
      curFiles.add(fileInfo);
      Path parentDir = new Path(outputFileName.toString()).getParent();
      if (finalPath == null)
      {
        do
        {
          groupNum++;
          finalPath = new Path(makeFinalPath(fileUID.get(), parentDir.toString(), groupId, getGroupIndex(groupNum)));
        } while ((finalPath.getFileSystem(getConf()).exists(finalPath)) && (group));
        if ((appendToLastFile) && (group))
        {
          Path tempPath = new Path(makeFinalPath(fileUID.get(), parentDir.toString(), groupId, getGroupIndex(groupNum - 1)));
          if (tempPath.getFileSystem(getConf()).exists(tempPath))
          {
            long tempFileSize = tempPath.getFileSystem(getConf()).getFileStatus(tempPath).getLen();
            if (tempFileSize + curSize < targetSize)
            {
              curSize += tempFileSize;
              finalPath = tempPath;
              groupNum--;
            }
          }
        }
      }
      if ((curSize >= targetSize) || (i == allFiles.size() - 1))
      {
        LOG.info("finalPath:" + finalPath);
        executeDownloads(this, curFiles, finalPath);
        curFiles = new ArrayList();
        curSize = 0L;
        groupNum++;
        finalPath = new Path(makeFinalPath(fileUID.get(), parentDir.toString(), groupId, getGroupIndex(groupNum)));
      }
    }
  }
  
  private String getGroupIndex(int groupNum)
  {
    if (groupNum == 0) {
      return "";
    }
    return Integer.toString(groupNum);
  }
  
  private void executeDownloads(CopyFilesReducer reducer, List<FileInfo> fileInfos, Path finalPath)
  {
    uncommittedFiles.addAll(fileInfos);
    for (FileInfo fileInfo : fileInfos) {
      LOG.info("Processing object: " + inputFileName.toString());
    }
    if (fileInfos.size() > 0)
    {
      LOG.info("Processing " + fileInfos.size() + " files");
      transferQueue.execute(new CopyFilesRunnable(reducer, fileInfos, finalPath));
    }
    else
    {
      LOG.info("No files to process");
    }
  }
  
  public void markFilesAsCommitted(List<FileInfo> fileInfos)
  {
    synchronized (uncommittedFiles)
    {
      LOG.info("Marking " + fileInfos.size() + " files as committed");
      for (FileInfo fileInfo : fileInfos) {
        LOG.info("Committing file: " + inputFileName);
      }
      uncommittedFiles.removeAll(fileInfos);
    }
    progress();
  }
  
  public InputStream decorateInputStream(InputStream inputStream, Path inputFilePath)
    throws IOException
  {
    String suffix = Utils.getSuffix(inputFilePath.getName()).toLowerCase();
    if ((suffix.equals("gz")) || (suffix.equals("gzip")))
    {
      FileSystem inputFs = inputFilePath.getFileSystem(conf);
      return new GZIPInputStream(new ByteCounterInputStream(inputStream, inputFs.getFileStatus(inputFilePath).getLen()));
    }
    if (suffix.equals("snappy"))
    {
      SnappyCodec codec = new SnappyCodec();
      codec.setConf(getConf());
      return codec.createInputStream(inputStream);
    }
    if ((suffix.equals("lzop")) || (suffix.equals("lzo")))
    {
      LzopCodec codec = new LzopCodec();
      codec.setConf(getConf());
      return codec.createInputStream(inputStream);
    }
    return inputStream;
  }
  
  public InputStream openInputStream(Path inputFilePath)
    throws IOException
  {
    FileSystem inputFs = inputFilePath.getFileSystem(conf);
    return inputFs.open(inputFilePath);
  }
  
  public OutputStream decorateOutputStream(OutputStream outputStream, Path outputFilePath)
    throws IOException
  {
    String suffix = Utils.getSuffix(outputFilePath.getName()).toLowerCase();
    if (("gz".equals(suffix)) || ("gzip".equals(suffix))) {
      return new GZIPOutputStream(outputStream);
    }
    if (("lzo".equals(suffix)) || ("lzop".equals(suffix)))
    {
      LzopCodec codec = new LzopCodec();
      codec.setConf(getConf());
      return codec.createOutputStream(outputStream);
    }
    if ("snappy".equals(suffix))
    {
      SnappyCodec codec = new SnappyCodec();
      codec.setConf(getConf());
      return codec.createOutputStream(outputStream);
    }
    return outputStream;
  }
  
  public OutputStream openOutputStream(Path outputFilePath)
    throws IOException
  {
    FileSystem outputFs = outputFilePath.getFileSystem(conf);
    OutputStream outputStream;
    OutputStream outputStream;
    if ((!Utils.isS3Scheme(outputFilePath.getFileSystem(conf).getUri().getScheme())) && (outputFs.exists(outputFilePath)) && (appendToLastFile)) {
      outputStream = outputFs.append(outputFilePath);
    } else {
      outputStream = outputFs.create(outputFilePath);
    }
    return outputStream;
  }
  
  public Progressable getProgressable()
  {
    return context;
  }
  
  public void progress()
  {
    context.progress();
  }
}

/* Location:
 * Qualified Name:     com.amazon.elasticmapreduce.s3distcp.CopyFilesReducer
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */