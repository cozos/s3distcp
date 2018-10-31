package com.amazon.elasticmapreduce.s3distcp;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;

public class FileInfoListing
{
  private static final Log LOG = LogFactory.getLog(FileInfoListing.class);
  private FileSystem fs;
  private SequenceFile.Writer writer;
  private Long fileIndex = Long.valueOf(0L);
  private long recordIndex = 0L;
  private Long recordsInThisFile = Long.valueOf(0L);
  private Long recordsPerFile;
  private Path tmpDir;
  private Configuration conf;
  private Path outputDir;
  private Path defaultSrcDir;
  private Pattern srcPattern;
  private Pattern groupBy;
  private OutputStream manifestStream;
  private Map<String, ManifestEntry> previousManifest;
  private final Gson gson;
  
  public FileInfoListing(Configuration conf, Path srcDir, Path tmpDir, Path outputDir, long startingIndex, File manifestFile, Map<String, ManifestEntry> previousManifest)
    throws IOException
  {
    this.conf = conf;
    defaultSrcDir = srcDir;
    this.tmpDir = tmpDir;
    this.outputDir = outputDir;
    recordsPerFile = Long.valueOf(500000L);
    recordIndex = startingIndex;
    if (manifestFile != null) {
      manifestStream = new GZIPOutputStream(new FileOutputStream(manifestFile));
    }
    GsonBuilder gsonBuilder = new GsonBuilder().disableHtmlEscaping();
    gson = gsonBuilder.create();
    this.previousManifest = previousManifest;
  }
  
  public void openNewFile()
  {
    try
    {
      if (writer != null) {
        writer.close();
      }
      fileIndex = Long.valueOf(fileIndex.longValue() + 1L);
      recordsInThisFile = Long.valueOf(0L);
      fs = FileSystem.get(tmpDir.toUri(), conf);
      Path path = new Path(tmpDir, fileIndex.toString());
      LOG.info("Opening new file: " + path.toString());
      writer = SequenceFile.createWriter(fs, conf, path, LongWritable.class, FileInfo.class, SequenceFile.CompressionType.NONE);
    }
    catch (IOException e)
    {
      throw new RuntimeException("Unable to open new file for writing" + new Path(tmpDir, fileIndex.toString()).toString(), e);
    }
  }
  
  public void add(Path filePath, long fileSize)
  {
    add(filePath, defaultSrcDir, fileSize);
  }
  
  public void add(Path filePath, Path srcDir, long fileSize)
  {
    String filePathString = filePath.toString();
    if (srcPattern != null)
    {
      Matcher matcher = srcPattern.matcher(filePathString);
      if (!matcher.matches()) {
        return;
      }
    }
    if (groupBy != null)
    {
      Matcher matcher = groupBy.matcher(filePathString);
      if (!matcher.matches()) {
        return;
      }
      int numGroups = matcher.groupCount();
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < numGroups; i++) {
        builder.append(matcher.group(i + 1));
      }
      if (builder.toString().length() == 0) {
        return;
      }
    }
    if ((writer == null) || (recordsInThisFile.longValue() > recordsPerFile.longValue())) {
      openNewFile();
    }
    recordIndex += 1L;
    recordsInThisFile = Long.valueOf(recordsInThisFile.longValue() + 1L);
    String outputFilePath = getOutputFilePath(filePath, srcDir);
    String baseName = getBaseName(filePath, srcDir);
    String manifestSrcDir = outputDir.toString();
    try
    {
      FileInfo fileInfo = new FileInfo(Long.valueOf(recordIndex), filePathString, outputFilePath, fileSize);
      
      LOG.debug("Adding " + fileInfo);
      if ((previousManifest == null) || (!previousManifest.containsKey(baseName)) || (previousManifest.get(baseName)).size != fileSize)) {
        writer.append(new LongWritable(recordIndex), fileInfo);
      }
      if (manifestStream != null)
      {
        ManifestEntry entry = new ManifestEntry(outputFilePath, baseName, manifestSrcDir, fileSize);
        
        String outLine = gson.toJson(entry) + "\n";
        manifestStream.write(outLine.getBytes("utf-8"));
      }
    }
    catch (IOException e)
    {
      throw new RuntimeException("Unable to write file copy entry " + filePathString, e);
    }
  }
  
  private String getBaseName(Path filePath, Path srcDir)
  {
    String filePathString = filePath.toString();
    String suffix = filePathString;
    String srcDirString = srcDir.toString();
    if (filePathString.startsWith(srcDirString))
    {
      suffix = filePathString.substring(srcDirString.length());
      if (suffix.startsWith("/")) {
        suffix = suffix.substring(1);
      }
    }
    return suffix;
  }
  
  private String getOutputFilePath(Path filePath, Path srcDir)
  {
    String suffix = getBaseName(filePath, srcDir);
    LOG.debug("outputDir: '" + outputDir + "'");
    LOG.debug("suffix: '" + suffix + "'");
    
    String outputPath = (suffix == null) || (suffix.isEmpty()) ? outputDir.toString() + "/" : new Path(outputDir, suffix).toString();
    
    LOG.debug("Output path: '" + outputPath + "'");
    return outputPath;
  }
  
  public void close()
  {
    try
    {
      if (writer != null) {
        writer.close();
      }
      if (manifestStream != null) {
        manifestStream.close();
      }
    }
    catch (IOException e)
    {
      throw new RuntimeException("Unable to close fileInfo writer", e);
    }
  }
  
  public Long getRecordsPerFile()
  {
    return recordsPerFile;
  }
  
  public void setRecordsPerFile(Long recordsPerFile)
  {
    this.recordsPerFile = recordsPerFile;
  }
  
  public Pattern getSrcPattern()
  {
    return srcPattern;
  }
  
  public void setSrcPattern(Pattern srcPattern)
  {
    this.srcPattern = srcPattern;
  }
  
  public Pattern getGroupBy()
  {
    return groupBy;
  }
  
  public void setGroupBy(Pattern groupBy)
  {
    this.groupBy = groupBy;
  }
  
  public Long getFileIndex()
  {
    return fileIndex;
  }
  
  public Long getRecordIndex()
  {
    return Long.valueOf(recordIndex);
  }
}

/* Location:
 * Qualified Name:     com.amazon.elasticmapreduce.s3distcp.FileInfoListing
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */