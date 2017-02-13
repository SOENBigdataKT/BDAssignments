package com.wtf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * *****************
 */
/**
 * FirstReducer used to produce inverted list  *
 */
/**
 * *****************
 */
public class FirstReducer extends Reducer<IntWritable,IntWritable,IntWritable,Text>{
	public void reduce(IntWritable key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
		
		IntWritable user=key;
		StringBuffer sb = new StringBuffer("");
		while (values.iterator().hasNext()) {
			Integer value = values.iterator().next().get();
			sb.append(value.toString() + " ");
		}
		Text followersAndFollowees = new Text(sb.toString());
		//emit the key-value pairs of followers and followees
		context.write(user, followersAndFollowees);   
	} 

}
