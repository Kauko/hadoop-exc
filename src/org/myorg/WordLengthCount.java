package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordLengthCount {
	public static class WordMap extends
			Mapper<LongWritable, Text, IntWritable, IntWritable> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
            Map<Integer, Integer> map = new HashMap<Integer, Integer>();
			String[] tokens = value.toString().split(" ");
			for(String token : tokens) {
                int tokenLength = token.length();
                if (map.containsKey(tokenLength)){
                    int total = map.get(token) +1;
                    map.put(tokenLength, total);
                }else{
                    map.put(tokenLength, 1);
                }


			}

            for (Map.Entry<Integer, Integer> item : map.entrySet()){
                context.write(new IntWritable(item.getKey()), new IntWritable(item.getValue()));
            }
		}
	}

	public static class Reduce extends 
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		// Run on a pseudo-distributed node 
		Configuration conf = new Configuration();
		Job job = new Job(conf, "wordcount");

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

		job.setJarByClass(WordLengthCount.class);

		job.setMapperClass(WordMap.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
