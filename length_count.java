package assign;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class length_count {
	
	public static class Map extends Mapper<LongWritable,Text,IntWritable,IntWritable>
	{
		public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{
			int len;
			String line= value.toString();
			StringTokenizer tokenizer=new StringTokenizer(line);
			while(tokenizer.hasMoreTokens())
			{
				len=(tokenizer.nextToken()).length();
				context.write(new IntWritable(len), new IntWritable(1));
			}
		}
	}
	public static class Reduce extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
	
		public void reduce(IntWritable key,Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum=0;
			for(IntWritable x: values)
			{
				sum+=x.get();
			}
			context.write(key, new IntWritable(sum));
			
			}
	}
		
	
	
	
	public static void main(String[] args) throws Exception{
		Configuration conf=new Configuration();
		Job job=new Job(conf,"mmsr");
		job.setJarByClass(length_count.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
			
		}	
		
	
	


}