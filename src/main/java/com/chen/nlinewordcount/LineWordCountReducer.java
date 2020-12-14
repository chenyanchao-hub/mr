package com.chen.nlinewordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// reducer的键值对输入与输出
public class LineWordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

	LongWritable v = new LongWritable();
	long sum = 0l;
//	重写reduce方法
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
//		汇总
		for(LongWritable value : values){
			sum += value.get();
		}
		v.set(sum);
		context.write(key, v);
	}	
}
