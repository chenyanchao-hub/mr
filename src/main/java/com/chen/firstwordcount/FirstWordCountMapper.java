package com.chen.firstwordcount;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//import org.apache.hadoop.io.LongWritable;



// map输入键值对，输出键值对
public class FirstWordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
	
//	定义键对变量
	Text k = new Text();
//	定义value值， 
	LongWritable v = new LongWritable(1);
	
//	重写map方法
	@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		String line = value.toString();
		String[] values = line.split(" ");
//		System.out.println(values[0]);
		k.set(values[0]);
		context.write(k, v);

		
		}
}
