package com.amazon.elasticmapreduce.s3distcp;

import com.google.gson.Gson;
import java.io.IOException;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ManifestIterator
  implements AutoCloseable
{
  private static final Log LOG = LogFactory.getLog(ManifestIterator.class);
  private Path manifestPath;
  private GZIPInputStream gzipStream;
  private Gson gson;
  private Scanner scanner;
  
  public ManifestIterator(Path manifestPath, Configuration conf)
    throws IOException
  {
    this.manifestPath = manifestPath;
    FileSystem fs = FileSystem.get(manifestPath.toUri(), conf);
    FSDataInputStream inStream = fs.open(manifestPath);
    gzipStream = new GZIPInputStream(new ByteCounterInputStream(inStream, fs.getFileStatus(manifestPath).getLen()));
    
    scanner = new Scanner(gzipStream);
    gson = new Gson();
  }
  
  public boolean hasNext()
  {
    return scanner.hasNextLine();
  }
  
  public ManifestEntry getNext()
    throws Exception
  {
    if (!scanner.hasNextLine()) {
      throw new IOException("Manifest iterator reached the end of file");
    }
    String line = scanner.nextLine();
    ManifestEntry entry = (ManifestEntry)gson.fromJson(line, ManifestEntry.class);
    return entry;
  }
  
  public void close()
  {
    try
    {
      gzipStream.close();
    }
    catch (IOException e)
    {
      LOG.warn("Failed to close stream for manifest file " + manifestPath, e);
    }
  }
}

/* Location:
 * Qualified Name:     com.amazon.elasticmapreduce.s3distcp.ManifestIterator
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */