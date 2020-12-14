package com.chen.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WorldcountDriver{
	public static void main(String[] args) throws IOException, ClassNotFoundException,InterruptedException{
		
		
//		1、读取Job对象
		
		Configuration conf = new Configuration();
//		设置切割符
		conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
		Job job = Job.getInstance(conf);
		
//		2、设置jar存储位置
		
		job.setJarByClass(WorldcountDriver.class);
		
//		3、关联Map和Reduce类
		
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);
		
//		4、设置Mapper阶段输出数据的key和value类型
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
//		设置输入格式
		job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat.class);
//		5、设置最终数据输出类型 key 和 value
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
//		6、 设置输入路径和输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
//		 更改切片方法
		 job.setInputFormatClass(CombineTextInputFormat.class);
//		 虚拟切片设置为4m
		 CombineTextInputFormat.setMaxInputSplitSize(job, 1024*1024*30);
		
//		7、提交job
		 boolean result = job.waitForCompletion(true);
		 
		 
//		 了解
		 System.exit(result?0:1);
		
	}

}
