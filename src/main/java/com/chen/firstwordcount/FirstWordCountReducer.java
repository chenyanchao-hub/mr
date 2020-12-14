package com.chen.firstwordcount;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// reduce 键值对输入， 键值对输出
public class FirstWordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	
	LongWritable v = new LongWritable();
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		
		long sum = 0L;
		
		for (LongWritable value : values) {
			sum += value.get();
		}
		
		v.set(sum);
		context.write(key, v);
	}
		
}
