package com.zhiyou.bd18.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class TotalSort {

	public static class TotalSortMap extends Mapper<LongWritable, Text, IntWritable, Text>{

		private String[] infos;
		private IntWritable oKey = new IntWritable();
		private Text oValue = new Text();
		
				
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			
			infos = value.toString().split("\\s");
			oKey.set(Integer.valueOf(infos[1]));
			oValue.set(infos[0]);
			context.write(oKey, oValue);
		}
	}
	
	public static class TotalSortReduce extends Reducer<IntWritable, Text, Text, IntWritable>{

		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(value, key);
			}
		}
		
	}
	public static class WritableDescComparetor extends IntWritable.Comparator{

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}

		
	}
	//定义全排序job
	public static void main(String[] args) throws Exception {
		//定义抽样
		Configuration configuration = new Configuration();
		//inputFormat传送到map或者sampler的数据key必须是我们想要抽样的key
		InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler(0.5, 5);
		//设置分区
		FileSystem hdfileSystem = FileSystem.get(configuration);
		Path path = new Path("/bd17/totalsort/_partition");
		//设置后,全排序的partition程序就会读取这个分区文件来完成按顺序进行分区
		TotalOrderPartitioner.setPartitionFile(configuration, path);
		//设置job
		Job job = Job.getInstance(configuration);
		job.setJarByClass(TotalSort.class);
		job.setJobName("全排序");
		job.setMapperClass(Mapper.class);
		job.setReducerClass(TotalSortReduce.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		//把分区文件加入分布式缓存中
		job.addCacheFile(path.toUri());
		//设置分区器
		job.setPartitionerClass(TotalOrderPartitioner.class);
		//设置reducer节点数
		job.setNumReduceTasks(2);
		
		//如果倒序排序的话方法之一指定job的sortcomparetor
		job.setSortComparatorClass(WritableDescComparetor.class);
		
		Path inputPath = new Path("/bd17/output/logincount");
		Path outputDir = new Path("/bd19/output/totalsort");
		hdfileSystem.delete(outputDir,true);
		//map端的输入会把文本文件读取成kv,按照分隔把一行分成两部分,前面key后面value
		//如果分隔符不存在则整行都是key,value 则为空,默认分隔符是\t,
		//手动指定分隔符参数: mapreduce.input.keyvaluelinerecordreader.key.value.separator.
		
		//job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		//将随机抽样写入分区文件
		InputSampler.writePartitionFile(job, sampler);
		//启动job
		System.exit(job.waitForCompletion(true)?0:1);
		
		
	}
 }
