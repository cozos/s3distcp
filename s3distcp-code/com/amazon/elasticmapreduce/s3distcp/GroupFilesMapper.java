package com.amazon.elasticmapreduce.s3distcp;

import java.io.IOException;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class GroupFilesMapper
  extends Mapper<LongWritable, FileInfo, Text, FileInfo>
{
  private static final Log log = LogFactory.getLog(GroupFilesMapper.class);
  protected Configuration conf;
  protected Pattern pattern = null;
  private String destDir;
  
  protected void setup(Mapper<LongWritable, FileInfo, Text, FileInfo>.Context context)
    throws IOException, InterruptedException
  {
    conf = context.getConfiguration();
    String patternString = conf.get("s3DistCp.listfiles.groupByPattern");
    if (patternString != null) {
      pattern = Pattern.compile(patternString);
    }
    destDir = conf.get("s3DistCp.copyfiles.destDir");
  }
  
  protected void map(LongWritable fileUID, FileInfo fileInfo, Mapper<LongWritable, FileInfo, Text, FileInfo>.Context context)
    throws IOException, InterruptedException
  {
    String path = new Path(inputFileName.toString()).toUri().getPath();
    if (path.startsWith(destDir)) {
      path = path.substring(destDir.length());
    }
    Text key = new Text(path);
    if (pattern != null)
    {
      Matcher matcher = pattern.matcher(inputFileName.toString());
      if (matcher.matches())
      {
        int numGroups = matcher.groupCount();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < numGroups; i++) {
          builder.append(matcher.group(i + 1));
        }
        key = new Text(builder.toString());
      }
    }
    log.debug("Adding " + key.toString() + ": " + inputFileName.toString());
    context.write(key, fileInfo);
  }
}

/* Location:
 * Qualified Name:     com.amazon.elasticmapreduce.s3distcp.GroupFilesMapper
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */