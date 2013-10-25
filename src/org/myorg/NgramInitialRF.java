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

        private Map<String,HashMap<String,Integer>> map;
        private static final int FLUSH_SIZE = 1000;

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

            //Split the input based on whitespace
            String[] tokens = value.toString().split("\\s+");
            /*
            Key <Key2, value>
            Key = the first character of the Ngram
            Key2 = the rest of the characters of the Ngram
            value = how many times Key+Key2 string Ngram appears

            This way it's easy for us to calculate the frequency!
             */
            Map<String, HashMap<String, Integer>> map = getMap();

            int n = Integer.parseInt(context.getConfiguration().get("N"));


            findKeys:
            for (int i = 0; i+(n-1) < tokens.length; i++){
                char letter = tokens[i].charAt(0);
                //If this string starts with a special character, ignore the string
                if (!Character.isLetter(letter))
                    continue findKeys;
                String sKey = "" + letter;
                String sValue = "";
                int j = 1;
                while (j < n){
                    letter = tokens[i+j].charAt(0);
                    //Again, ignore strings that start with special character
                    if (!Character.isLetter(letter))
                        continue findKeys;
                    sValue +=  letter + " ";
                    j++;
                }

                //Remove empty space
                sValue = sValue.substring(0, sValue.length()-1);

                if (!map.containsKey(sKey))
                    map.put(sKey, new HashMap<String, Integer>());

                HashMap<String, Integer> values = map.get(sKey);
                if (values.containsKey(sValue))
                    values.put(sValue, values.get(sValue)+1);
                else
                    values.put(sValue, 1);


            }

            flush(context, false);
		}

        private void flush(Context context, boolean force)
                throws IOException, InterruptedException {
            Map<String, HashMap<String, Integer>> map = getMap();
            if(!force) {
                int size = map.size();
                if(size < FLUSH_SIZE)
                    return;
            }

            for (Map.Entry<String, HashMap<String, Integer>> item : map.entrySet()){
                MapWritable ret = new MapWritable();
                for (Map.Entry<String, Integer> valueMap : item.getValue().entrySet()){
                    ret.put(new Text(valueMap.getKey()), new IntWritable(valueMap.getValue()));
                    /*if (Integer.parseInt(context.getConfiguration().get("MAP_SIZE")) > 0 && ret.size() > Integer.parseInt(context.getConfiguration().get("MAP_SIZE"))){
                        context.write(new Text(item.getKey()), ret);
                        ret.clear();
                    }*/
                }
                context.write(new Text(item.getKey()), ret);

            }

            map.clear();
        }

        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            flush(context, true);
        }

        public Map<String,HashMap<String,Integer>> getMap() {
            if(null == map)
                map = new HashMap<String,HashMap<String,Integer>>();
            return map;
        }
    }

	public static class Reduce extends 
			Reducer<Text, MapWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterable<MapWritable> values,
				Context context) throws IOException, InterruptedException {
			HashMap<String, Double> ret = new HashMap<String, Double>();
            int totalNum = 0;
            double limit = Double.parseDouble(context.getConfiguration().get("P"));

            // values = all maps associated with this key
            for (MapWritable map : values){
                //item = key: rest of the Ngram, value: total amount of this Ngram
                for (Map.Entry<Writable, Writable> item : map.entrySet()){
                    String sKey = key + " " + item.getKey();
                    double value = ((IntWritable)item.getValue()).get();
                    //The total number of Ngrams starting with "key" character
                    totalNum += value;
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
