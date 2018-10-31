package com.amazon.elasticmapreduce.s3distcp;

public class ManifestEntry
{
  public String path;
  public String baseName;
  public String srcDir;
  public long size;
  
  public ManifestEntry() {}
  
  public ManifestEntry(String path, String baseName, String srcDir, long size)
  {
    this.path = path;
    this.baseName = baseName;
    this.srcDir = srcDir;
    this.size = size;
  }
}

/* Location:
 * Qualified Name:     com.amazon.elasticmapreduce.s3distcp.ManifestEntry
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */