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
 * SecondMapper to filter the followers     *
 */
/**
 * *****************
 */
public class SecondMapper extends Mapper<Object,Text,IntWritable,IntWritable>{
	public void map(Object key,Text values,Context context) throws IOException,InterruptedException{
		StringTokenizer stObj = new StringTokenizer(values.toString().substring(2));
		
		//user will store the list of values.
		Integer user = Integer.parseInt(values.toString().substring(0,1));
		while(stObj.hasMoreTokens()){
			Integer acc1=Integer.parseInt(stObj.nextToken().toString().trim());
			StringTokenizer stObjCopy = new StringTokenizer(values.toString().substring(2));
			while(stObjCopy.hasMoreTokens()){
				Integer acc2=Integer.parseInt(stObjCopy.nextToken().toString().trim());
		//Filtering out the existing followers
				if((!(acc1==acc2))&&(!(acc1<0||acc2<0))){
			//emit (acc1,acc2)
					context.write(new IntWritable(acc1),new IntWritable(acc2));	
				}
			}
			if(acc1<0)
				//emit (user,acc1)
				context.write(new IntWritable(user), new IntWritable(acc1));
		}
	}
}
