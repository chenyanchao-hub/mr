package com.chen.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


// 输入输出
// KEYIN VALUEIN 是mapper阶段输出的k-v对

public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	IntWritable v = new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
			int sum = 0;
//			累加求和
			for(IntWritable value : values){
				sum += value.get();
			}
//			写入求和数据
			v.set(sum);
			context.write(key, v);
	}

}
