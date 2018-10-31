package com.amazon.elasticmapreduce.s3distcp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ToolRunner;

public class Main
{
  private static final Log log = LogFactory.getLog(S3DistCp.class);
  
  public static void main(String[] args)
    throws Exception
  {
    String str = "";
    if ((args != null) && (args.length > 0))
    {
      StringBuilder sb = new StringBuilder();
      for (String s : args) {
        sb.append(s).append(" ");
      }
      str = sb.toString();
    }
    log.info("Running with args: " + str);
    
    System.exit(ToolRunner.run(new S3DistCp(), args));
  }
}

/* Location:
 * Qualified Name:     com.amazon.elasticmapreduce.s3distcp.Main
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */