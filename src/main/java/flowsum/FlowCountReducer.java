package flowsum;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


//reducer输入k v对、输出 k v 对
public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean>{

	@Override
	protected void reduce(Text key, Iterable<FlowBean> values,Context context)
			throws IOException, InterruptedException {
		
		long sum_upFlow = 0;
		long sum_downFlow = 0;
		for(FlowBean flowBean : values){
			sum_upFlow += flowBean.getUpFlow();
			sum_downFlow += flowBean.getDownFlow();
		}
		
//		封装对象
		FlowBean resultBean = new FlowBean(sum_upFlow, sum_downFlow);
		
//		写出
		context.write(key, resultBean);
	}
}
