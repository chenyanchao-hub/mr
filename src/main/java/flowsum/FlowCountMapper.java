package flowsum;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// mapper输入键值对，以及输出键值对
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
	
//	设置封装对象
	FlowBean v = new FlowBean();
	Text k = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
//			获取一行
		String line = value.toString();
		
//		切割字段
		String[] fields = line.split("\t");
		System.out.println(fields.length);
		System.out.println(fields.toString());
//		封装对象
//		取出手机号
		k.set(fields[1]);
//		取出上行流量和下行流量
		Long upFlow = Long.parseLong(fields[fields.length-3]);
		Long downFlow = Long.parseLong(fields[fields.length-2]);
		v.setDownFlow(downFlow);
		v.setUpFlow(upFlow);
		v.setSumFlow(upFlow+downFlow);
		
//		写出
		context.write(k, v);
		

	}
}
