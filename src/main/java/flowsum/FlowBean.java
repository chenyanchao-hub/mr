package flowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable{
	
	private Long upFlow;
	private Long downFlow;
	private Long sumFlow;
	
	
//	构造一个空参构造方法
	public FlowBean() {
		super();
	}
	

	public FlowBean(Long upFlow, Long downFlow) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		sumFlow = upFlow + downFlow;
	}

//  序列化
	@Override
	public void write(DataOutput out) throws IOException {
		 out.writeLong(upFlow);
		 out.writeLong(downFlow);
		 out.writeLong(sumFlow);
		
	}
	
//	反序列化 ：反序列化的顺序与序列化的顺序一致
	@Override
	public void readFields(DataInput in) throws IOException {
		upFlow = in.readLong();
		downFlow = in.readLong();
		sumFlow = in.readLong();
		
	}


	@Override
	public String toString() {
		return upFlow + "\t" + downFlow + "\t" + sumFlow;
	}


	public Long getUpFlow() {
		return upFlow;
	}


	public void setUpFlow(Long upFlow) {
		this.upFlow = upFlow;
	}


	public Long getDownFlow() {
		return downFlow;
	}


	public void setDownFlow(Long downFlow) {
		this.downFlow = downFlow;
	}


	public Long getSumFlow() {
		return sumFlow;
	}


	public void setSumFlow(Long sumFlow) {
		this.sumFlow = sumFlow;
	}
}
