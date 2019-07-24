package com.hadoopcourse;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordPartitioner extends Partitioner<Text, LongWritable>{
	@Override
	public int getPartition(Text key, LongWritable value, int noOfReducers) {
		// TODO Auto-generated method stub
		
		String word = key.toString().toLowerCase();
		char firstChar = word.charAt(0);
		int diff = Math.abs(firstChar - 'a') % noOfReducers;
		return diff;

	}

}
