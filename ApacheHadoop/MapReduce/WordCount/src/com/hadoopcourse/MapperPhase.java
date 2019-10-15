package com.hadoopcourse;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperPhase extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		// Hi, This is sample hadoop input file.
		String line = value.toString();

		String[] words = line.split(" ");

		for (String word : words)

		{

			Text outputKey = new Text(word.toUpperCase().trim());

			IntWritable outputValue = new IntWritable(1);
			// hi,1
			// this,1

			context.write(outputKey, outputValue);
			// (hi,1)
			// (this,1)
			// (is,1)

		}
	}

}
