package com.amazon.elasticmapreduce.s3distcp;

import java.util.concurrent.ThreadFactory;

final class Utils$1
  implements ThreadFactory
{
  private int threadCount = 1;
  
  Utils$1(String paramString) {}
  
  public Thread newThread(Runnable r)
  {
    Thread thread = new Thread(r);
    thread.setName(val$threadName + "-" + threadCount++);
    thread.setDaemon(true);
    return thread;
  }
}

/* Location:
 * Qualified Name:     com.amazon.elasticmapreduce.s3distcp.Utils.1
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */