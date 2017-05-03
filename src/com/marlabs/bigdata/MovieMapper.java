package com.marlabs.bigdata;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String s[] = value.toString().split("\t");
	
		context.write(new IntWritable(Integer.parseInt(s[2])), new IntWritable(1)); 
		
	}

	

}
