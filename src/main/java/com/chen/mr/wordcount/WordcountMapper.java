package com.chen.mr.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



//map阶段 
// KEYIN :输入数据类型
// VALUE :输入数据的值
// KEYOUT：输出数据的类型
// VALUE :输出数据的值
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	Text k =new Text();
	IntWritable v = new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
			
//		1 获取一行
		 String line = value.toString();
		 
//		2切分单词
		String[] words = line.split(" ");
//		3循环写出
		
		for(String word : words){
			
			k.set(word);
			context.write(k, v);
		}
	}

}
