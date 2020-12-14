package com.chen.firstwordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FirstWordCountDriver {

	public static void main(String[] args) throws IOException, InterruptedException,ClassNotFoundException{
		

		//		1、创建一个Job对象
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		//		2、关键Jar包位置
		job.setJarByClass(FirstWordCountDriver.class);

		// 3、关联map reduce
		job.setMapperClass(FirstWordCountMapper.class);
		job.setReducerClass(FirstWordCountReducer.class);
		
		// 4、设置map输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		// 5、设置最终输出数据类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		//	6、设置输入输出路径
		FileInputFormat.setInputPaths(job, new Path("e:/winput"));
		FileOutputFormat.setOutputPath(job, new Path("e:/woutput"));

		// 7、提交job
		boolean state = job.waitForCompletion(true);
//		系统打印
		System.exit(state?0:1);
	}
}
