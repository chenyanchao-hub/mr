package com.chen.nlinewordcount;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;





// mapper的键值对输入输出
public class LineWordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

//	设置map键值对的值value
	long v = 1;
	Text k = new Text();
	LongWritable val = new LongWritable();
	
//	重写map方法
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
//		获取第一行
		String line = value.toString();
//		使用“ ” 切分单词
		String[] words = line.split(" ");
		for(String word : words){
			k.set(word);
			val.set(v);
			context.write(k,val);
		}
	}
}
