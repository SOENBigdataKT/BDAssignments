package com.wtf;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FirstMapper extends Mapper<Object,Text,IntWritable,IntWritable>{
	public void map(Object key,Text values,Context context) throws IOException,InterruptedException{
		StringTokenizer stObj = new StringTokenizer(values.toString());
		Integer user=Integer.parseInt(stObj.nextToken());
		while(stObj.hasMoreTokens()){
			Integer followee=Integer.parseInt(stObj.nextToken());
			context.write(new IntWritable(followee),new IntWritable(user));
			context.write(new IntWritable(user),new IntWritable(-followee));
		}
	}
}

