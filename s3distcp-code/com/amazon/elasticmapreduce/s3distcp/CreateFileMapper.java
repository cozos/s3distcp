package com.amazon.elasticmapreduce.s3distcp;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CreateFileMapper
  extends Mapper<LongWritable, CreateFileInfo, LongWritable, CreateFileInfo>
{
  protected Mapper<LongWritable, CreateFileInfo, LongWritable, CreateFileInfo>.Context conf;
  
  protected void setup(Mapper<LongWritable, CreateFileInfo, LongWritable, CreateFileInfo>.Context context)
    throws IOException, InterruptedException
  {
    conf = conf;
  }
  
  protected void map(LongWritable key, CreateFileInfo value, Mapper<LongWritable, CreateFileInfo, LongWritable, CreateFileInfo>.Context context)
    throws IOException, InterruptedException
  {
    try
    {
      FileSystem fs = FileSystem.get(new URI(fileName.toString()), context.getConfiguration());
      FSDataOutputStream outputFile = fs.create(new Path(fileName.toString()));
      long bytesLeftToWrite = fileSize.get();
      byte[] buffer = new byte[12582912];
      for (int i = 0; (i < buffer.length) && (i < bytesLeftToWrite); i++) {
        buffer[i] = ((byte)(i % 127));
      }
      while (bytesLeftToWrite > buffer.length)
      {
        outputFile.write(buffer);
        bytesLeftToWrite -= buffer.length;
        context.progress();
      }
      if (bytesLeftToWrite > 0L)
      {
        outputFile.write(buffer, 0, (int)bytesLeftToWrite);
        bytesLeftToWrite = 0L;
      }
    }
    catch (URISyntaxException e)
    {
      throw new RuntimeException(e);
    }
  }
}

/* Location:
 * Qualified Name:     com.amazon.elasticmapreduce.s3distcp.CreateFileMapper
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */