package com.chen.nlinewordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LineWordCountDriver {

	
	public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException{
		// TODO Auto-generated method stub
		// 创建job
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		//设置jar路径
		job.setJarByClass(LineWordCountDriver.class);
		//关联map和reduce
		job.setMapperClass(com.chen.nlinewordcount.LineWordCountMapper.class);
		job.setReducerClass(com.chen.nlinewordcount.LineWordCountReducer.class);
		//设置map的key、value输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		//设置最终输出key、value输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		//设置输入输出、路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 设置每个切片里有三个记录
		NLineInputFormat.setNumLinesPerSplit(job, 3);
		// 设置使用NLineInputFormat处理记录数
		job.setInputFormatClass(NLineInputFormat.class);
		
		//提交Job
		boolean result = job.waitForCompletion(true);
		
		System.exit(result?0:1);
	}

}
