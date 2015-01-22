package nl.utwente.bigdata;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.json.simple.parser.JSONParser;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import nl.utwente.bigdata.Controller;
import java.text.Normalizer;

public class TwitterSentiment {

	public static Controller controller= new Controller();

	public static class ExampleMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private Text idString = new Text();
		private Text tweetText = new Text();
		private JSONParser parser = new JSONParser();
		private Map tweet;
		

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				tweet = (Map<String, Object>) parser
						.parse(value.toString());
			} catch (ClassCastException e) {
				return; // do nothing (we might log this)
			} catch (org.json.simple.parser.ParseException e) {
				return; // do nothing
			}
			
			//filter all tweets that are not english
			if(((String)tweet.get("lang")).equals("en")){
				return;
			}
			//filter all tweets that do not have a football player name in them
			String tweetString =((String) tweet.get("text")).replaceAll("\n", " ");
			tweetString = tweetString.toLowerCase();
			tweetString = tweetString.trim();
			tweetString = tweetString.replaceAll("[^a-zA-Z0-9\\s]", "");
			tweetString = 
       Normalizer
           .normalize(tweetString, Normalizer.Form.NFD)
           .replaceAll("[^\\p{ASCII}]", "");
			if(tweetString==null || tweetString.length()<2){
				return;
			}
			String playername = controller.hasPlayer(tweetString);
			if(playername.isEmpty()){
				return;
			}

			idString.set(playername);
			
			int result= controller.getMood(tweetString);
			context.write(idString, new IntWritable(result));
		}
	}

	public static class ExampleReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum = sum + value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: TwitterSentiment <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "TwitterSentiment");
		job.setJarByClass(TwitterSentiment.class);
		job.setMapperClass(ExampleMapper.class);
		job.setReducerClass(ExampleReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
