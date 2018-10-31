package com.amazon.elasticmapreduce.s3distcp;

import java.util.concurrent.Callable;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

class S3DistCp$1
  implements Callable<Void>
{
  S3DistCp$1(S3DistCp paramS3DistCp, Configuration paramConfiguration, Pair paramPair, FileInfoListing paramFileInfoListing) {}
  
  public Void call()
    throws Exception
  {
    this$0.createInputFileListS3(val$conf, ((Path)val$prefix.getFirst()).toUri(), val$fileInfoListing, ((Boolean)val$prefix.getSecond()).booleanValue());
    return null;
  }
}

/* Location:
 * Qualified Name:     com.amazon.elasticmapreduce.s3distcp.S3DistCp.1
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */