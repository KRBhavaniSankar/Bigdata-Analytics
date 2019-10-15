package com.bhavani.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class DepartmentPartitioner extends Partitioner<Text, Text>

{

	@Override

	public int getPartition(Text key, Text value, int numReduceTasks)

	{

		String emp_dept = key.toString();

		if (numReduceTasks == 0)

			return 0;

		if (key.equals(new Text("Program Department")))

		{

			return 0;

		}

		else if (key.equals(new Text("Admin Department")))

		{

			return 1 % numReduceTasks;

		}

		else

			return 2 % numReduceTasks;

	}

}
