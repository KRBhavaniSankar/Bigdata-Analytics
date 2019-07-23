package com.hadoopcourse;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerPhase extends Reducer<Text, IntWritable, Text, LongWritable> {
	
	@Override
	protected void reduce(Text word, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		int sum = 0;

		   for(IntWritable value : values)

		   {

		   sum += value.get();

		   }

		   context.write(word, new LongWritable(sum));

		
		
	}

}
