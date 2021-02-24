package datacloud.hadoop.lastfm;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;





public class lastfm {
	//UserId TrackId LocalListening RadioListening Skip
	public static class lastfmMapperJob1 
	extends Mapper<LongWritable, Text, Text, IntWritable>{
		private String[] splited_value;
		//TrackId #listener
		//1. le nombre de personnes qui l’ont écouté au moins une fois (en local ou en radio)
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//user  track  locallisting   raio            skip
			//user4 track1 16              8               3
			splited_value = value.toString().split("\\s");
			context.write(new Text(splited_value[1]), new IntWritable(1));
		}
	}
	
	public static class lastfmReducerJob1 extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	
	
	//----------------------------------------------------------------------
	//JOB 2
	//TrackId #listening #skips
	public static class lastfmMapperJob2 
	extends Mapper<LongWritable, Text, Text, Text>{
		private String[] splited_value;
		//2. le nombre de fois où il a été écouté et passé sans écoute.
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//user  track  locallisting   raio            skip
			//user4 track1 16              8               3
			splited_value = value.toString().split("\\s");
			context.write(new Text(splited_value[1]), new Text(splited_value[2]+" "+splited_value[3]));

		}
	}
	
	public static class lastfmReducerJob2 extends Reducer<Text,Text,Text,Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			Integer sum_lis = 0,sum_skip=0;
			for (Text val : values) {
				String[] splited= val.toString().split("\\s");
				sum_lis+=Integer.parseInt(splited[0]);
				sum_skip=Integer.parseInt(splited[1]);
			}	
			context.write(key, new Text(sum_lis.toString()+" "+sum_skip.toString()));
		}
	}
	
	//----------------------------------------------------------------------
	//JOB 3
	public static class lastfmMapperJob3 
	extends Mapper<LongWritable, Text, Text, Text>{
		private String[] splited_value;
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			splited_value = value.toString().split("\\s");
			if(splited_value.length==2)
				context.write(new Text(splited_value[0]), new Text(splited_value[1]));
			else if(splited_value.length==3)
				context.write(new Text(splited_value[0]), new Text(splited_value[1]+" "+splited_value[2]));
		}
	}
	public static class lastfmReducerJob3 extends Reducer<Text,Text,Text,Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//trackid      le reste
			HashMap<String, String[]> hm = new HashMap<>();
			for (Text val : values) {
				String[] splited= val.toString().split("\\s");
				if(splited.length == 2) {
					if(hm.containsKey(splited[0])) {
						String[] v = hm.get(splited[0]);
						v[0]=splited[1];
						hm.replace(splited[0], v);
					}
					else {
						String[] v = new String[3];
						v[0]=splited[1];
						hm.put(splited[0], v);
					}
				}
				else {
					if(hm.containsKey(splited[0])) {
						String[] v = hm.get(splited[0]);
						v[1]=splited[1];
						v[2]=splited[2];
						hm.replace(splited[0], v);
					}
					else {
						String[] v = new String[3];
						v[1]=splited[1];
						v[2]=splited[2];
						hm.put(splited[0], v);
					}
				}
			}
			
			StringBuilder s = new StringBuilder();
			for (Entry<String, String[]> entry : hm.entrySet()) {
			    s.append(entry.getKey()).append(" ").append(entry.getValue()[0]).append(" ").append(entry.getValue()[1]).append(" ").append(entry.getValue()[2]).append('\n');    
			}
			context.write(key, new Text(s.toString()));
		}
	}

	//----------------------------------------------------------------------



	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.map.speculative", false);
		conf.setBoolean("mapreduce.reduce.speculative", false);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: lastfm <in> <out>");
			System.exit(2);
		}
		Job job1 = Job.getInstance(conf, "lastfm-1");
		Job job2 = Job.getInstance(conf,"lastfm-2");
		Job job3 = Job.getInstance(conf,"lastfm-3");
		//------------------------------------
		job1.setJarByClass(lastfm.class);
		job2.setJarByClass(lastfm.class);
		job3.setJarByClass(lastfm.class);
		//------------------------------------
		job1.setMapperClass(lastfmMapperJob1.class);
		job2.setMapperClass(lastfmMapperJob2.class);
		job3.setMapperClass(lastfmMapperJob3.class);
		//------------------------------------
		job1.setReducerClass(lastfmReducerJob1.class);
		job2.setReducerClass(lastfmReducerJob2.class);
		job3.setReducerClass(lastfmReducerJob3.class);
		//------------------------------------
		job1.setMapOutputKeyClass(Text.class);
		job2.setMapOutputKeyClass(Text.class);
		job3.setMapOutputKeyClass(Text.class);
		//------------------------------------
		job1.setMapOutputValueClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		//------------------------------------
		job1.setOutputKeyClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job3.setOutputKeyClass(Text.class);
		//------------------------------------
		job1.setOutputValueClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);
		job3.setOutputValueClass(Text.class);
		//------------------------------------
		job1.setInputFormatClass(TextInputFormat.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job3.setInputFormatClass(TextInputFormat.class);
		//------------------------------------
		job1.setOutputFormatClass(TextOutputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		//------------------------------------
		//job.setPartitionerClass(HashPartitioner.class);
		//------------------------------------
		job1.setNumReduceTasks(1);
		job2.setNumReduceTasks(1);
		job3.setNumReduceTasks(1);
		
		//------------------------------------
		MultipleInputs.addInputPath(job1, new Path(otherArgs[0]),TextInputFormat.class,lastfmMapperJob1.class);
		MultipleInputs.addInputPath(job2, new Path(otherArgs[0]),TextInputFormat.class,lastfmMapperJob2.class);
		MultipleInputs.addInputPath(job3, new Path(otherArgs[1]),TextInputFormat.class,lastfmMapperJob3.class);
		MultipleInputs.addInputPath(job3, new Path(otherArgs[1]+"/res2"),TextInputFormat.class,lastfmMapperJob3.class);
		//indique le chemin du dossier de sortie
		final Path outDir = new Path(otherArgs[1]);
		final Path outDir2 = new Path(otherArgs[1]+"/res2");
		final Path outDir3 = new Path(otherArgs[1]+"/res3");
		
		FileOutputFormat.setOutputPath(job1, outDir);
		FileOutputFormat.setOutputPath(job2, outDir2);
		FileOutputFormat.setOutputPath(job3, outDir3);
		
		final FileSystem fs = FileSystem.get(conf);//récupération d'une référence sur le système de fichier HDFS
		
		if (fs.exists(outDir)) fs.delete(outDir, true); 
		if (fs.exists(outDir2)) fs.delete(outDir2, true);
		if (fs.exists(outDir3)) fs.delete(outDir3, true);
		
		// soumission de l'application à Yarn	
		job1.waitForCompletion(true);
		job2.waitForCompletion(true);
		
		
		
		//supprimer les sucess
		fs.delete(new Path(otherArgs[1]+"/_SUCCESS"), true);
		fs.delete(new Path(otherArgs[1]+"/res2/_SUCCESS"), true);
		
		System.exit(job3.waitForCompletion(true) ? 0 : 1);
	}
}
