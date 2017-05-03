package com.marlabs.bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MovieMain {
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		
		if(args.length!=2){
			System.err.println("invalid");
			System.exit(0);
		}
		
		Configuration config = new Configuration();
		Job job = new Job(config,"MovieMain");
		
		job.setJarByClass(MovieMain.class);
		job.setMapperClass(MovieMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);	
		
		job.setReducerClass(MovieReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
		
	}

}
