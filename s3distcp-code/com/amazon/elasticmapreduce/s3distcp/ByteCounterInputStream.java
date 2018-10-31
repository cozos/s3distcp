package com.amazon.elasticmapreduce.s3distcp;

import java.io.IOException;
import java.io.InputStream;

public class ByteCounterInputStream
  extends InputStream
{
  private final InputStream inputStream;
  private final long contentLength;
  private long bytesRead;
  private long mark;
  
  public ByteCounterInputStream(InputStream inputStream, long contentLength)
  {
    this.inputStream = inputStream;
    bytesRead = 0L;
    this.contentLength = contentLength;
    mark = 0L;
  }
  
  public int read()
    throws IOException
  {
    int ret = inputStream.read();
    if (ret >= 0) {
      bytesRead += 1L;
    }
    return ret;
  }
  
  public int read(byte[] b)
    throws IOException
  {
    int ret = inputStream.read(b);
    if (ret >= 0) {
      bytesRead += ret;
    }
    return ret;
  }
  
  public int read(byte[] b, int off, int len)
    throws IOException
  {
    int ret = inputStream.read(b, off, len);
    if (ret >= 0) {
      bytesRead += ret;
    }
    return ret;
  }
  
  public long skip(long n)
    throws IOException
  {
    long ret = inputStream.skip(n);
    if (ret >= 0L) {
      bytesRead += ret;
    }
    return ret;
  }
  
  public int available()
    throws IOException
  {
    return (int)(contentLength - bytesRead);
  }
  
  public void close()
    throws IOException
  {
    inputStream.close();
  }
  
  public synchronized void mark(int readlimit)
  {
    mark = bytesRead;
    inputStream.mark(readlimit);
  }
  
  public synchronized void reset()
    throws IOException
  {
    if (markSupported()) {
      bytesRead = mark;
    }
    inputStream.reset();
  }
  
  public boolean markSupported()
  {
    return inputStream.markSupported();
  }
}

/* Location:
 * Qualified Name:     com.amazon.elasticmapreduce.s3distcp.ByteCounterInputStream
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */