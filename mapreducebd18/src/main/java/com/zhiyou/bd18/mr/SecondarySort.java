package com.zhiyou.bd18.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondarySort {

	//自定义封装类型,封装二次排序的第一个和第二个字段
	//自定义排序规则:第一个字段不同按照第一个字段排序,第一个字段相同按照第二个字段排序
	public static class Twofileds implements WritableComparable<Twofileds>{

		private String firstFileld;
		private int secondField;
		
		public String getFirstFileld() {
			return firstFileld;
		}

		public void setFirstFileld(String firstFileld) {
			this.firstFileld = firstFileld;
		}

		public int getSecondField() {
			return secondField;
		}

		public void setSecondField(int secondField) {
			this.secondField = secondField;
		}

		//序列化
		public void write(DataOutput out) throws IOException {
			
			out.writeUTF(firstFileld);
			out.writeInt(secondField);
		}

		//反序列化
		public void readFields(DataInput in) throws IOException {
			this.firstFileld = in.readUTF();
			this.secondField = in.readInt();
		}

		//比较方法
		//先比较第一个字段,第一个字段相同的在用第二个字段的比较结果
		public int compareTo(Twofileds o) {

			if (this.firstFileld.equals(o.firstFileld)) {
				
//				if (this.secondField>o.secondField) {
//					return 1;
//				}else if (this.secondField<o.secondField) {
//					return -1;
//				}else {
//					return 0;
//				}
				return this.secondField-o.secondField;
				
			}else {
				return this.firstFileld.compareTo(o.firstFileld);
			}
			
		}
	}
	
	//自定义分区,用来将第一个字段相同的key值分区到同一个reducer节点上
	public static class TwoFieldsPartitioner extends Partitioner<Twofileds, NullWritable>{
		//返回值是一个int数字,这个数字是reducer的标号
		@Override
		public int getPartition(Twofileds key, NullWritable value, int numPartitions) {
			
			int reducerNo = (key.firstFileld.hashCode()&Integer.MAX_VALUE )% numPartitions;
			
			
			return reducerNo;
		}
		
	}
	
	//定义map
	
	public static class SecondarySortMap extends Mapper<Text, Text, Twofileds, NullWritable>{
		private final NullWritable oValue =NullWritable.get();

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Twofileds, NullWritable>.Context context)
				throws IOException, InterruptedException {
			//将两个字段中的数据封装到一个twoFields 对象中
			Twofileds twoFields = new Twofileds();
			twoFields.setFirstFileld(key.toString());
			twoFields.setSecondField(Integer.valueOf(value.toString()));
			context.write(twoFields, oValue);
		}
		
	}
	//定义reducer
	public static class SecondarySortReducer extends Reducer<Twofileds, NullWritable, Text, Text>{
		private Text oKey = new Text();
		private Text oValue = new Text();
		@Override
		protected void reduce(Twofileds key, Iterable<NullWritable> values,
				Reducer<Twofileds, NullWritable, Text, Text>.Context context) throws IOException, InterruptedException {
			for (NullWritable value : values) {
				oKey.set(key.firstFileld);
				oValue.set(String.valueOf(key.secondField));
				context.write(oKey, oValue);
			}
			oKey.set("----------");
			oValue.set("---------");
			context.write(oKey, oValue);
		}
		
	}
 	//定义分组比较器,让不同key值的第一个字段相同的kv调用同一个reducer方法
	
	public static class GroupToReducerComparetor extends WritableComparator{
		//构造方法里面要向父类传递比较器要比较的数据类型
		 public GroupToReducerComparetor() {
			 //1.比较器比较的类型参数
			 //2.是否实例化对象
			 super(Twofileds.class,true);
		}
		//重写compare方法自定义排序规则
		@Override
		public int compare(WritableComparable a, WritableComparable b) {

			Twofileds ca = (Twofileds)a;
			Twofileds cb = (Twofileds)b;
			return ca.getFirstFileld().compareTo(cb.getFirstFileld());
		}
		
		
	}
	//设置job
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		job.setJarByClass(SecondarySort.class);
		job.setJobName("二次排序");
		
		job.setMapperClass(SecondarySortMap.class);
		job.setReducerClass(SecondarySortReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Twofileds.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		
		Path inputPath = new Path("/bd17/output/secondaryorder");
		Path outputDir = new Path("/bd19/output/sort");
		outputDir.getFileSystem(configuration).delete(outputDir, true);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		//把文件内容以kv的形式读取出来发送给map
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		//设置partition
		job.setPartitionerClass(TwoFieldsPartitioner.class);
		//设置分组比较器
		job.setGroupingComparatorClass(GroupToReducerComparetor.class);
		
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
	
	
	
}
