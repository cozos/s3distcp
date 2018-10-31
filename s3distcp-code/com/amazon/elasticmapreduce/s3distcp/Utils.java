package com.amazon.elasticmapreduce.s3distcp;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import java.net.URI;
import java.security.SecureRandom;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Utils
{
  private static final Log LOG = LogFactory.getLog(Utils.class);
  
  public static String randomString(long value)
  {
    StringBuffer result = new StringBuffer();
    if (value < 0L) {
      value = -value;
    }
    do
    {
      long remainder = value % 58L;
      int c;
      int c;
      if (remainder < 24L)
      {
        c = 'a' + (char)(int)remainder;
      }
      else
      {
        int c;
        if (remainder < 48L) {
          c = 'A' + (char)(int)(remainder - 24L);
        } else {
          c = '0' + (char)(int)(remainder - 48L);
        }
      }
      result.appendCodePoint(c);
      value /= 58L;
    } while (value > 0L);
    return result.reverse().toString();
  }
  
  public static String randomString()
  {
    return randomString(new SecureRandom().nextLong());
  }
  
  public static String getSuffix(String name)
  {
    if (name != null)
    {
      String[] parts = name.split("\\.");
      if (parts.length > 1) {
        return parts[(parts.length - 1)];
      }
    }
    return "";
  }
  
  public static String replaceSuffix(String name, String suffix)
  {
    int index = name.lastIndexOf('.');
    return name.substring(0, index) + suffix;
  }
  
  public static String appendSuffix(String name, String suffix)
  {
    return name + suffix;
  }
  
  public static boolean isS3Scheme(String scheme)
  {
    return ("s3".equals(scheme)) || ("s3n".equals(scheme));
  }
  
  public static String cleanupColonsAndSlashes(String s)
  {
    if (Strings.isNullOrEmpty(s)) {
      return s;
    }
    s = s.replaceAll(":", "/");
    return Joiner.on("/").skipNulls().join(Splitter.on("/").trimResults().omitEmptyStrings().split(s));
  }
  
  public static ThreadPoolExecutor createExecutorService(String threadName, int numWorkers)
  {
    ThreadFactory threadFactory = new ThreadFactory()
    {
      private int threadCount = 1;
      
      public Thread newThread(Runnable r)
      {
        Thread thread = new Thread(r);
        thread.setName(val$threadName + "-" + threadCount++);
        thread.setDaemon(true);
        return thread;
      }
    };
    LOG.info("Created executor service " + threadName + " with " + numWorkers + " worker threads");
    ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor)Executors.newFixedThreadPool(numWorkers, threadFactory);
    return threadPoolExecutor;
  }
  
  public static String uriToBucket(URI uri)
  {
    return uri.getAuthority();
  }
}

/* Location:
 * Qualified Name:     com.amazon.elasticmapreduce.s3distcp.Utils
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */