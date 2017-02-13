package com.wtf;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * *****************
 */
/**
 * FirstMapper  to do indexing of users to get  followees   *
 */
/**
 * *****************
 * *This class generates key values pairs for the reducer
 */
public class FirstMapper extends Mapper<Object,Text,IntWritable,IntWritable>{
	public void map(Object key,Text values,Context context) throws IOException,InterruptedException{
		//splitting the values into token
		StringTokenizer stObj = new StringTokenizer(values.toString());
		//Storing the value as key
		Integer user=Integer.parseInt(stObj.nextToken());
		while(stObj.hasMoreTokens()){
			Integer followee=Integer.parseInt(stObj.nextToken());
			//emit key-value pairs of followee
			context.write(new IntWritable(followee),new IntWritable(user));
			context.write(new IntWritable(user),new IntWritable(-followee));
		}
	}
}