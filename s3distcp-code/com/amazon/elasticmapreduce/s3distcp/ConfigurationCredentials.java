package com.amazon.elasticmapreduce.s3distcp;

import com.amazonaws.auth.AWSCredentials;
import org.apache.hadoop.conf.Configuration;

public class ConfigurationCredentials
  implements AWSCredentials
{
  String awsAccessKeyId;
  String awsAWSSecretKey;
  
  String getConfigOrThrow(Configuration configuration, String key)
  {
    String value = configuration.get(key);
    if (value == null) {
      throw new RuntimeException("Unable to retrieve Hadoop configuration for key " + key);
    }
    return value;
  }
  
  public ConfigurationCredentials(Configuration configuration)
  {
    awsAccessKeyId = getConfigOrThrow(configuration, "fs.s3n.awsAccessKeyId");
    awsAWSSecretKey = getConfigOrThrow(configuration, "fs.s3n.awsSecretAccessKey");
  }
  
  public String getAWSAccessKeyId()
  {
    return awsAccessKeyId;
  }
  
  public String getAWSSecretKey()
  {
    return awsAWSSecretKey;
  }
}

/* Location:
 * Qualified Name:     com.amazon.elasticmapreduce.s3distcp.ConfigurationCredentials
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */