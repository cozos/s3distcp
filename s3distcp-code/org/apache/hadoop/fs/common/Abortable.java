package org.apache.hadoop.fs.common;

import java.io.IOException;

public abstract interface Abortable
{
  public abstract void abort()
    throws IOException;
}

/* Location:
 * Qualified Name:     org.apache.hadoop.fs.common.Abortable
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */