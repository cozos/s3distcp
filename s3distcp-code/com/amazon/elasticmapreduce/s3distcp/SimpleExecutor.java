package com.amazon.elasticmapreduce.s3distcp;

import java.util.concurrent.Executor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SimpleExecutor
  implements Executor
{
  private static final Log LOG = LogFactory.getLog(Worker.class);
  protected boolean closed;
  protected int tail;
  protected int head;
  protected Exception lastException;
  protected Runnable[] queue;
  protected Thread[] workers;
  
  static class Worker
    implements Runnable
  {
    private final SimpleExecutor executor;
    
    Worker(SimpleExecutor executor)
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
            SimpleExecutor.LOG.error("Worker task threw exception", e);
          }
        }
      }
      catch (InterruptedException localInterruptedException) {}
    }
  }
  
  public SimpleExecutor(int queueSize, int workerSize)
  {
    queue = new Runnable[queueSize + 1];
    workers = new Thread[workerSize];
    head = 0;
    tail = 0;
    closed = false;
    lastException = null;
    startWorkers();
  }
  
  public synchronized void registerException(Exception e)
  {
    lastException = e;
  }
  
  public synchronized void assertNoExceptions()
  {
    if (lastException != null) {
      throw new RuntimeException("Some tasks in remote executor failed", lastException);
    }
  }
  
  private void startWorkers()
  {
    for (int i = 0; i < workers.length; i++)
    {
      workers[i] = new Thread(new Worker(this));
      workers[i].setName("s3distcp-simpler-executor-worker-" + i);
      workers[i].start();
    }
  }
  
  public void close()
  {
    synchronized (this)
    {
      closed = true;
      notifyAll();
    }
    for (int i = 0; i < workers.length; i++) {
      try
      {
        workers[i].join();
      }
      catch (InterruptedException e)
      {
        LOG.error("Interrupted while waiting for workers", e);
      }
    }
  }
  
  public synchronized boolean closed()
  {
    return closed;
  }
  
  public synchronized void execute(Runnable command)
  {
    try
    {
      while (isFull()) {
        wait();
      }
    }
    catch (InterruptedException e)
    {
      throw new RuntimeException(e);
    }
    queue[head] = command;
    head = ((head + 1) % queue.length);
    notifyAll();
  }
  
  synchronized boolean isEmpty()
  {
    return head == tail;
  }
  
  synchronized boolean isFull()
  {
    return (head + 1) % queue.length == tail;
  }
  
  synchronized int size()
  {
    int result = head - tail;
    if (result < 0) {
      return result + queue.length;
    }
    return result;
  }
  
  public synchronized Runnable take()
    throws InterruptedException
  {
    while ((isEmpty()) && (!closed)) {
      wait(15000L);
    }
    if (!isEmpty())
    {
      Runnable returnItem = queue[tail];
      tail = ((tail + 1) % queue.length);
      notifyAll();
      return returnItem;
    }
    return null;
  }
}

/* Location:
 * Qualified Name:     com.amazon.elasticmapreduce.s3distcp.SimpleExecutor
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */