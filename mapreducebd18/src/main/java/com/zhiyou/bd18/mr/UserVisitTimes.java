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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;



//计算每个用户对该系统进行登录访问(Login)的次数
public class UserVisitTimes {
	
	public static class LoginCountMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		private String[] infos;
		private Text oKey=new Text();
		private final IntWritable oValue= new IntWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			//解析一行数据，转换成一个单词组成的数组
			infos =	value.toString().split("\\s");
			/*for(String i:infos){
				//把单词形成一个kv对发送给reducer（单词，1）
				oKey.set(i);
				context.write(oKey, oValue);			
			}*/
             if (infos[1].equals("login")){
            	 oKey.set(infos[0]);
            	 context.write(oKey, oValue);
				
			}           
		}

	}
	//定义reducer
	public static class LoginCountReducer extends Reducer<Text, IntWritable, IntWritable, Text>{

		private  int sum;
		private IntWritable oValue= new IntWritable(0);
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, IntWritable, Text>.Context context) throws IOException, InterruptedException {

			sum = 0;
			for(IntWritable value:values){
				sum +=value.get();				
			}
			//输出kv(单词，单词的计数)
			oValue.set(sum);
			context.write(oValue, key);

		}

	}
	//组装一个job到mr引擎上执行
	public static void main(String[] args) throws Exception{
		//构建一个configuration，用来配置hdfs的位置，和mr的各项参数
		Configuration configuration = new Configuration();
		//创建job
		Job job =Job.getInstance(configuration);
		job.setJarByClass(UserVisitTimes.class);
		job.setJobName("统计登陆次数");

		//配置mr执行类
		job.setMapperClass(LoginCountMap.class);
		job.setReducerClass(LoginCountReducer.class);

		//设置输出kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		//设置数据源（待处理）
		Path inputpath = new Path("/user-logs-large.txt");
		FileInputFormat.addInputPath(job, inputpath);

		//设置目标数据的存放位置
		Path outputPath = new Path("/bd17/output/logincount");
		//解决不能重复
		
		outputPath.getFileSystem(configuration).delete(outputPath,true);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(job, outputPath);

		//启动作业，分布式计算提交给mr引擎
		boolean result = job.waitForCompletion(true);
		System.exit(result?0:1);


	}
}
