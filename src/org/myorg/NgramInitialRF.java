package org.myorg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class NgramInitialRF {
	public static class WordMap extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
            Map<String, Integer> tempMap = new HashMap<String, Integer>();
            Map<String, Integer> map = new HashMap<String, Integer>();
			String[] tokens = value.toString().split(" ");
            int n = Integer.parseInt(context.getConfiguration().get("N"));
			for(int i = 0; i+(n-1) < tokens.length ; i++) {
                int j = 0;
                String sKey = "";

                while (j < n){
                    sKey += tokens[i+j].charAt(0) + " ";
                    j++;
                }

                sKey = sKey.substring(0, sKey.length()-1);

                if (tempMap.containsKey(sKey)){
                    int total = tempMap.get(sKey) +1;
                    tempMap.put(sKey, total);
                }else{
                    tempMap.put(sKey, 1);
                }


			}

            for( Map.Entry<String,Integer> item : tempMap.entrySet()){
                char firstChar = item.getKey().charAt(0);
                int total = 0;
                for (String s : tokens){
                    if (s.charAt(0) == firstChar)
                        total++;
                }
                int freq = item.getValue() / total;
                map.put(item.getKey(), freq);
            }

            for (Map.Entry<String, Integer> item : map.entrySet()){
                context.write(new Text(item.getKey()), new IntWritable(item.getValue()));
            }
		}
	}

	public static class Reduce extends 
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
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
        conf.set("N", args[2]);
		Job job = new Job(conf, "nGramInitialRF");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

		job.setJarByClass(NgramInitialRF.class);

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
