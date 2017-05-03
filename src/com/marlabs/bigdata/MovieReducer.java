package com.marlabs.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
	
	@Override
	public void reduce(IntWritable key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException{
		
		int cnt=0;
		for(IntWritable values:value){
			
			cnt = cnt + values.get();	
		}
		
		context.write(key,new IntWritable(cnt));	
	}
}
