package com.amazon.elasticmapreduce.s3distcp;

import org.apache.commons.logging.Log;

class SimpleExecutor$Worker
  implements Runnable
{
  private final SimpleExecutor executor;
  
  SimpleExecutor$Worker(SimpleExecutor executor)
  {
    this.executor = executor;
  }
  
  public void run()
  {
    try
    {
      Runnable job;
      while ((job = executor.take()) != null) {
        try
        {
          job.run();
        }
        catch (RuntimeException e)
        {
          executor.registerException(e);
          SimpleExecutor.access$000().error("Worker task threw exception", e);
        }
      }
    }
    catch (InterruptedException localInterruptedException) {}
  }
}

/* Location:
 * Qualified Name:     com.amazon.elasticmapreduce.s3distcp.SimpleExecutor.Worker
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */