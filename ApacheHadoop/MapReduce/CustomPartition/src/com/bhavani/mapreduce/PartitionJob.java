package com.bhavani.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PartitionJob extends Configured implements Tool {

	public int run(String[] arg) throws Exception

	{

		Configuration conf = getConf();

		Job job = new Job(conf, "Maxsal");

		job.setJarByClass(PartitionJob.class);
		FileInputFormat.setInputPaths(job, new Path(arg[0]));

		FileOutputFormat.setOutputPath(job, new Path(arg[1]));

		job.setMapperClass(MapperPhase.class);

		job.setMapOutputKeyClass(Text.class);

		job.setMapOutputValueClass(Text.class);

		job.setPartitionerClass(DepartmentPartitioner.class);

		job.setReducerClass(ReducerPhase.class);

		job.setNumReduceTasks(3);

		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		return 0;

	}

	public static void main(String[] args) throws Exception

	{

		int res = ToolRunner.run(new Configuration(), new PartitionJob(), args);

		if (args.length != 2)

		{

			System.err.println("Usage: SamplePartitioner <input> <output>");

			System.exit(2);

		}

		System.exit(res);

	}

}
