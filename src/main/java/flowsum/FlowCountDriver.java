package flowsum;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class FlowCountDriver {

	public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException{
		
		// 		输入路径 输出路径		
		args = new String[]{"e:/input","e:/output"};
		//		1 获取Job对象
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		//		2 设置jar路径
		job.setJarByClass(FlowCountDriver.class);

		//		3关联 map reduce
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);

		//		4 设置map输出key和value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		//		5设置最终的key和value输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		//		6设置输入输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//		7 提交Job
		
		boolean result = job.waitForCompletion(true);

		System.exit(result?0:1);

	}

}
