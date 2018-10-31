package com.amazon.elasticmapreduce.s3distcp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CreateSampleData$CreateFileReducer
  extends Reducer<LongWritable, CreateFileInfo, LongWritable, CreateFileInfo>
{}

/* Location:
 * Qualified Name:     com.amazon.elasticmapreduce.s3distcp.CreateSampleData.CreateFileReducer
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */