package com.zhiyou.bd18.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserVisitTimess {

	public static class UserVisitTimesMap extends Mapper<LongWritable, Text, IntWritable, Text>{
		private String[] infos;
		private IntWritable oKey = new IntWritable();
		private Text oValue = new Text();
		
	
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			infos = value.toString().split("\\s");
			oKey.set(Integer.parseInt(infos[1]));
			oValue.set(infos[0]);
			context.write(oKey, oValue);
		}
		
		
	}
	
	public static class UserVisitTimesReducer extends Reducer<IntWritable, Text, Text, IntWritable>{

		
		//private IntWritable oValue = new IntWritable();
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			for (Text text : values) {
				context.write(text, key);
			}
		}
		
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		job.setJarByClass(UserVisitTimess.class);
		job.setJobName("计算登陆次数顺序排序");
		
		//设置mr执行类
		job.setMapperClass(UserVisitTimesMap.class);
		job.setReducerClass(UserVisitTimesReducer.class);
		
		//设置输出kv类型
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		//设置数据源
		
		Path inputpath = new Path("/bd17/output/logincount/part-r-00000");
		FileInputFormat.addInputPath(job, inputpath);
		
		//设置目标数据的存放位置
		Path outputPath = new Path("/bd17/output/logincountgg");
		outputPath.getFileSystem(configuration).delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		//启动作业
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
