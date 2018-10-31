package com.amazon.elasticmapreduce.s3distcp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

abstract class WritableStruct
  implements Writable
{
  public abstract Writable[] getFields();
  
  public void readFields(DataInput input)
    throws IOException
  {
    for (Writable field : getFields()) {
      field.readFields(input);
    }
  }
  
  public void write(DataOutput output)
    throws IOException
  {
    for (Writable field : getFields()) {
      field.write(output);
    }
  }
}

/* Location:
 * Qualified Name:     com.amazon.elasticmapreduce.s3distcp.WritableStruct
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */