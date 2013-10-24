package org.myorg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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

public class NgramInitialRF {
	public static class WordMap extends
			Mapper<LongWritable, Text, Text, MapWritable> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
            Map<String,HashMap<String,Integer>> map = new HashMap<String,HashMap<String,Integer>>();
            String[] temp = value.toString().split("\\s+");
            ArrayList<String> tokens = new ArrayList<String>();
            for (String s : temp){
                if(s.length() > 0)
                    tokens.add(s);
            }
            int n = Integer.parseInt(context.getConfiguration().get("N"));


            findKeys:
            for (int i = 0; i+(n-1) < tokens.size() ; i++){
                char letter = tokens.get(i).charAt(0);
                if (!Character.isAlphabetic(letter))
                    continue findKeys;
                String sKey = "" + letter;
                String sValue = "";
                int j = 1;
                while (j < n){
                    letter = tokens.get(i+j).charAt(0);
                    if (!Character.isAlphabetic(letter))
                        continue findKeys;
                    sValue +=  letter + " ";
                    j++;
                }

                sValue = sValue.substring(0, sValue.length()-1);

                if (!map.containsKey(sKey))
                    map.put(sKey, new HashMap<String, Integer>());

                HashMap<String, Integer> values = map.get(sKey);
                if (values.containsKey(sValue))
                    values.put(sValue, values.get(sValue)+1);
                else
                    values.put(sValue, 1);


            }

            for (Map.Entry<String, HashMap<String, Integer>> item : map.entrySet()){
                MapWritable ret = new MapWritable();
                for (Map.Entry<String, Integer> valueMap : item.getValue().entrySet()){
                      ret.put(new Text(valueMap.getKey()), new IntWritable(valueMap.getValue()));
                }
                context.write(new Text(item.getKey()), ret);

            }
		}
	}

	public static class Reduce extends 
			Reducer<Text, MapWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterable<MapWritable> values,
				Context context) throws IOException, InterruptedException {
			HashMap<String, Double> ret = new HashMap<String, Double>();
            int totalNum = 0;
            double limit = Double.parseDouble(context.getConfiguration().get("P"));

            for (MapWritable map : values){
                totalNum += map.size();
                for (Map.Entry<Writable, Writable> item : map.entrySet()){
                    String sKey = key + " " + item.getKey();
                    double value = ((IntWritable)item.getValue()).get();
                    if(ret.containsKey(sKey))
                        ret.put(sKey, ret.get(sKey)+value);
                    else
                        ret.put(sKey, value);
                }
            }

            for (Map.Entry<String, Double> item : ret.entrySet()){
                item.setValue(item.getValue() / totalNum);
                if (item.getValue() >= limit )
                    context.write(new Text(item.getKey()), new DoubleWritable(item.getValue()));

            }



			/*for (DoubleWritable val : values) {
				total = (total != 0) ? total * val.get() : val.get();
            }
            double limit = Double.parseDouble(context.getConfiguration().get("P"));
            if(total >= limit)
			    context.write(key, new DoubleWritable(total));*/
		}
	}

	public static void main(String[] args) throws Exception {
		// Run on a pseudo-distributed node 
		Configuration conf = new Configuration();
        conf.set("N", args[2]);
        conf.set("P", args[3]);
		Job job = new Job(conf, "nGramInitialRF");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);

		job.setJarByClass(NgramInitialRF.class);

		job.setMapperClass(WordMap.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
