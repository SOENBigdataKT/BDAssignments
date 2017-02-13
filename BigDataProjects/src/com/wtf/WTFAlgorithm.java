package com.wtf;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WTFAlgorithm {
	 /**
     * *****************
     */
    /**
     * Who to Follow Controller Class to handle Map-reduce jobs      *
     */
    /**
     * *****************
     */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		try {
			Configuration config = new Configuration();
			//First Map-reduce Job
			Job firstJob = Job.getInstance(config, "WTFAlgorithm First Job");
			firstJob.setJarByClass(WTFAlgorithm.class);
			firstJob.setMapperClass(FirstMapper.class);
			firstJob.setReducerClass(FirstReducer.class);
			firstJob.setOutputKeyClass(IntWritable.class);
			firstJob.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(firstJob, new Path(args[0]));
			FileSystem fsObj = FileSystem.get(config);
			if(fsObj.exists(new Path(args[1]))){
				fsObj.delete(new Path(args[1]),true);
			}
			FileOutputFormat.setOutputPath(firstJob, new Path(args[1]));
			if(firstJob.waitForCompletion(true)){
				//Second Map-reduce Job
				Job secondJob = Job.getInstance(config, "WTFAlgorithm Second Job");
				secondJob.setMapperClass(SecondMapper.class);
				secondJob.setReducerClass(SecondReducer.class);
				secondJob.setOutputKeyClass(IntWritable.class);
				secondJob.setOutputValueClass(IntWritable.class);
				FileInputFormat.addInputPath(secondJob, new Path(args[1]));
				if(fsObj.exists(new Path(args[2]))){
					fsObj.delete(new Path(args[2]),true);
				}
				FileOutputFormat.setOutputPath(secondJob, new Path(args[2]));
				secondJob.waitForCompletion(true);			
			}
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}


}