package com.amazon.elasticmapreduce.s3distcp;

import java.io.IOException;
import java.net.URI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CreateSampleData
  implements Tool
{
  private static final Log LOG = LogFactory.getLog(CreateSampleData.class);
  protected Configuration conf;
  
  public static void main(String[] args)
    throws Exception
  {
    CreateSampleData distcp = new CreateSampleData();
    int result = ToolRunner.run(distcp, args);
    System.exit(result);
  }
  
  public CreateSampleData()
    throws IOException
  {
    Configuration jobConf = getConf();
    conf = jobConf;
  }
  
  void createFileList(Path inputFileListPath, String inputLocation, URI inputUri, URI outputUri)
    throws IOException
  {
    FileSystem inputFS = FileSystem.get(inputUri, conf);
    FileSystem inputFileListFS = FileSystem.get(inputFileListPath.toUri(), conf);
    Path inputPath = new Path(inputLocation);
    
    LongWritable uid = new LongWritable(1L);
    
    inputFileListFS.delete(inputFileListPath, true);
    inputFileListFS.mkdirs(inputFileListPath);
    SequenceFile.Writer fileInfoWriter = SequenceFile.createWriter(inputFileListFS, conf, inputFileListPath, LongWritable.class, FileInfo.class, SequenceFile.CompressionType.NONE);
    try
    {
      FileStatus[] contents = inputFS.listStatus(inputPath);
      for (FileStatus child : contents)
      {
        String inputFilePath = child.getPath().toString();
        String outputFilePath = join(outputUri.toString(), child.getPath().getName());
        FileInfo info = new FileInfo(Long.valueOf(uid.get()), inputFilePath, outputFilePath, child.getLen());
        fileInfoWriter.append(uid, info);
        uid.set(uid.get() + 1L);
      }
    }
    finally
    {
      fileInfoWriter.close();
    }
    FileStatus[] fileListContents = inputFileListFS.listStatus(inputFileListPath);
    for (FileStatus status : fileListContents) {
      LOG.info("fileListContents: " + status.getPath());
    }
  }
  
  private void createInputFiles(String inputPathString, long numFiles, long fileSize, String outputPath)
  {
    try
    {
      FileSystem fs = FileSystem.get(new URI(inputPathString), conf);
      fs.mkdirs(new Path(inputPathString));
      for (int fileNumber = 1; fileNumber <= numFiles; fileNumber++)
      {
        String inputFileName = join(inputPathString, Integer.valueOf(fileNumber));
        Path inputFilePath = new Path(inputFileName);
        fs.delete(inputFilePath, true);
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, inputFilePath, LongWritable.class, CreateFileInfo.class, SequenceFile.CompressionType.NONE);
        try
        {
          writer.append(new LongWritable(fileNumber), new CreateFileInfo(join(outputPath, Integer.valueOf(fileNumber)), fileSize));
        }
        finally
        {
          writer.close();
        }
      }
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }
  }
  
  public Configuration getConf()
  {
    return conf;
  }
  
  private String join(String s, Integer t)
  {
    return join(s, t.toString());
  }
  
  private String join(String s, String t)
  {
    if ((s.length() != 0) && (s.charAt(s.length() - 1) == '/')) {
      return s + t;
    }
    return s + "/" + t;
  }
  
  public int run(String[] args)
    throws Exception
  {
    String outputLocation = args[0];
    
    long numFiles = conf.getLong("createSampleData.numFiles", 5L);
    long fileSize = conf.getLong("createSampleData.fileSize", 104857600L);
    String jobName = conf.get("createSampleData.baseJobName", "CreateSampleData");
    String tmpPathString = conf.get("createSampleData.tmpDir", "hdfs:///tmp/createSampleData");
    String inputPathString = conf.get("createSampleData.workingInputDir", join(tmpPathString, "input"));
    String outputPathString = conf.get("createSampleData.workingOutputDir", join(tmpPathString, "output"));
    
    FileSystem.get(new URI(outputPathString), conf).delete(new Path(outputPathString), true);
    
    createInputFiles(inputPathString, numFiles, fileSize, outputLocation);
    return runCreateJob(inputPathString, outputPathString, jobName);
  }
  
  int runCreateJob(String inputPathString, String outputPathString, String jobName)
  {
    int ret = -1;
    try
    {
      Job job = Job.getInstance(conf);
      job.setJarByClass(getClass());
      job.setJobName(jobName);
      job.setMapSpeculativeExecution(false);
      
      FileInputFormat.addInputPath(job, new Path(inputPathString));
      FileOutputFormat.setOutputPath(job, new Path(outputPathString));
      
      job.setInputFormatClass(SequenceFileInputFormat.class);
      job.setOutputKeyClass(LongWritable.class);
      job.setOutputValueClass(CreateFileInfo.class);
      job.setMapperClass(CreateFileMapper.class);
      job.setReducerClass(CreateFileReducer.class);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
      job.submit();
      
      ret = job.waitForCompletion(true) ? 0 : -1;
    }
    catch (IOException e)
    {
      LOG.error(e.getMessage(), e);
    }
    catch (InterruptedException e)
    {
      LOG.error(e.getMessage(), e);
    }
    catch (ClassNotFoundException e)
    {
      LOG.error(e.getMessage(), e);
    }
    return ret;
  }
  
  public void setConf(Configuration conf)
  {
    this.conf = conf;
  }
  
  public static class CreateFileReducer
    extends Reducer<LongWritable, CreateFileInfo, LongWritable, CreateFileInfo>
  {}
}

/* Location:
 * Qualified Name:     com.amazon.elasticmapreduce.s3distcp.CreateSampleData
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */