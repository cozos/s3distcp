package com.amazon.elasticmapreduce.s3distcp;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.util.Progressable;

public class ProgressableResettableBufferedFileInputStream
  extends InputStream
{
  protected File file;
  protected Progressable progressable;
  private BufferedInputStream inputStream;
  private long mark = 0L;
  private long pos = 0L;
  
  public ProgressableResettableBufferedFileInputStream(File file, Progressable progressable)
    throws IOException
  {
    this.file = file;
    this.progressable = progressable;
    inputStream = new BufferedInputStream(new FileInputStream(file));
  }
  
  public int available()
    throws IOException
  {
    return inputStream.available();
  }
  
  public void close()
    throws IOException
  {
    inputStream.close();
  }
  
  public synchronized void mark(int readlimit)
  {
    if (progressable != null) {
      progressable.progress();
    }
    mark = pos;
  }
  
  public boolean markSupported()
  {
    if (progressable != null) {
      progressable.progress();
    }
    return true;
  }
  
  public int read()
    throws IOException
  {
    if (progressable != null) {
      progressable.progress();
    }
    int read = inputStream.read();
    if (read != -1) {
      pos += 1L;
    }
    return read;
  }
  
  public int read(byte[] b, int off, int len)
    throws IOException
  {
    if (progressable != null) {
      progressable.progress();
    }
    int read = inputStream.read(b, off, len);
    if (read != -1) {
      pos += read;
    }
    return read;
  }
  
  public int read(byte[] b)
    throws IOException
  {
    if (progressable != null) {
      progressable.progress();
    }
    int read = inputStream.read(b);
    if (read != -1) {
      pos += read;
    }
    return read;
  }
  
  public synchronized void reset()
    throws IOException
  {
    if (progressable != null) {
      progressable.progress();
    }
    inputStream.close();
    inputStream = new BufferedInputStream(new FileInputStream(file));
    pos = inputStream.skip(mark);
  }
  
  public long skip(long n)
    throws IOException
  {
    if (progressable != null) {
      progressable.progress();
    }
    long skipped = inputStream.skip(n);
    pos += skipped;
    return skipped;
  }
}

/* Location:
 * Qualified Name:     com.amazon.elasticmapreduce.s3distcp.ProgressableResettableBufferedFileInputStream
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */