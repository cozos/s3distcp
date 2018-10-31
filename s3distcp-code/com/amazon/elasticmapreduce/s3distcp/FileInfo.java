package com.amazon.elasticmapreduce.s3distcp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

class FileInfo
  extends WritableStruct
  implements Cloneable
{
  public LongWritable fileUID = new LongWritable(0L);
  public Text inputFileName = new Text();
  public Text outputFileName = new Text();
  public LongWritable fileSize = new LongWritable(0L);
  
  public FileInfo() {}
  
  public FileInfo(Long fileUID, String inputUri, String outputUri, long fileSize)
  {
    this.fileUID = new LongWritable(fileUID.longValue());
    inputFileName = new Text(inputUri);
    outputFileName = new Text(outputUri);
    this.fileSize = new LongWritable(fileSize);
  }
  
  public FileInfo clone()
  {
    return new FileInfo(Long.valueOf(fileUID.get()), inputFileName.toString(), outputFileName.toString(), fileSize.get());
  }
  
  public Writable[] getFields()
  {
    return new Writable[] { fileUID, inputFileName, outputFileName, fileSize };
  }
  
  public String toString()
  {
    return "{" + fileUID + ", '" + inputFileName + "', '" + outputFileName + "', " + fileSize + "}";
  }
}

/* Location:
 * Qualified Name:     com.amazon.elasticmapreduce.s3distcp.FileInfo
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */