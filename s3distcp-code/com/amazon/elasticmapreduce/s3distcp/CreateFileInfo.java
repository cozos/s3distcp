package com.amazon.elasticmapreduce.s3distcp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

class CreateFileInfo
  extends WritableStruct
  implements Cloneable
{
  public Text fileName = new Text();
  public LongWritable fileSize = new LongWritable();
  
  public CreateFileInfo() {}
  
  public CreateFileInfo(String fileName, long fileSize)
  {
    this.fileName = new Text(fileName);
    this.fileSize = new LongWritable(fileSize);
  }
  
  public CreateFileInfo clone()
  {
    return new CreateFileInfo(fileName.toString(), fileSize.get());
  }
  
  public Writable[] getFields()
  {
    return new Writable[] { fileName, fileSize };
  }
}

/* Location:
 * Qualified Name:     com.amazon.elasticmapreduce.s3distcp.CreateFileInfo
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */