package com.zhiyou.bd18.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//计算出访问过该系统的每一个用户的用户名
//去重
public class DesDumplicate {
	//从文件中抽取用户名,并将key发送到reducer节点,value与计算无关
	//,设置NullWritable类型
	public static class DeDumplicateMap extends Mapper<LongWritable, Text, Text, NullWritable>{
		private String[] infos;
		private NullWritable oValue = NullWritable.get();
		private Text oKey = new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			//解析一行记录
			infos = value.toString().split("\\s");
			oKey.set(infos[0]);
			context.write(oKey, oValue);
			
		}
	}
	public static class DeDumplicateReduce extends Reducer<Text, NullWritable, Text, NullWritable>{

		private final NullWritable oValue = NullWritable.get();

		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			context.write(key, oValue);
		}
	}
	
	//构建和启动job
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		job.setJarByClass(DesDumplicate.class);
		job.setJobName("计算访问过系统的用户名");
		
		job.setMapperClass(DeDumplicateMap.class);
		job.setReducerClass(DeDumplicateReduce.class);
		
		//设置map的输出kv类型和整个mrjob的map输出kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		//设置输入数据
		Path inputPath = new Path("/user-logs-large.txt");
		FileInputFormat.addInputPath(job, inputPath);
		
		//设置输出数据
		Path outputPath = new Path("/bd17/output/desdump");
		outputPath.getFileSystem(configuration).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
