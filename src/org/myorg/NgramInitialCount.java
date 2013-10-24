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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class NgramInitialCount {


	public static class WordMap extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
            Map<String, Integer> map = new HashMap<String, Integer>();
			String[] temp = value.toString().split("\\s+");
            ArrayList<String> tokens = new ArrayList<String>();
            for (String s : temp){
                if(s.length() > 0)
                  tokens.add(s);
            }

            int n = Integer.parseInt(context.getConfiguration().get("N"));

            findKeys:
			for(int i = 0; i+(n-1) < tokens.size() ; i++) {
                int j = 0;
                String sKey = "";

                while (j < n){
                    Character letter = tokens.get(i+j).charAt(0);
                    if (!Character.isLetter(letter))
                         continue findKeys;
                    sKey += letter + " ";
                    j++;
                }

                sKey = sKey.substring(0, sKey.length()-1);

                if (map.containsKey(sKey)){
                    int total = map.get(sKey) +1;
                    map.put(sKey, total);
                }else{
                    map.put(sKey, 1);
                }


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

		Job job = new Job(conf, "nGramInitialCount");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

		job.setJarByClass(NgramInitialCount.class);

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
