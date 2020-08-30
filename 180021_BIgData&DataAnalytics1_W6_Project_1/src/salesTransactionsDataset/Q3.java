package salesTransactionsDataset;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

//Q.3 Find out which Payment_Type was used max no. of times.                                                                                                 (10 Marks)
//Here, it can find the payment mode which maximum number of times is used by the people.

public class Q3 {
	//mapper class
		public static class MapForWordCount extends Mapper<LongWritable,Text,Text,IntWritable>
		{
			public void map(LongWritable key,Text value, Context con) throws IOException, InterruptedException
			{
				if (key.get() == 0){
					return;
				}else{
				String line= value.toString();
				String[] words=line.split(",");
				
					Text outputkey=new Text(words[3]);
					IntWritable outputvalue=new IntWritable(1);
					con.write(outputkey,outputvalue);
				
				}
			}
			
		}
		//reducer class
		public static class ReduceForWordCount extends Reducer<Text,IntWritable,Text,IntWritable>
		{
			Map<Text, IntWritable> hmaps = new HashMap<>();
			
			public void reduce(Text word,Iterable<IntWritable> values,Context con)throws IOException, InterruptedException
			{
				int sum=0;
				for(IntWritable value:values)
				{
					sum=sum+value.get();
				}
				hmaps.put(word, new IntWritable(sum));
				
			//	con.write(word,new IntWritable(sum));
			}
			public void cleanup(Context con) throws IOException, InterruptedException{
				Map.Entry<Text, IntWritable> highestPaymentValue = null; 
				   
		        for (Map.Entry<Text, IntWritable>latestValue : hmaps.entrySet()) {
		        	if (highestPaymentValue == null ||latestValue.getValue().compareTo(highestPaymentValue.getValue())> 0) { 
		        		highestPaymentValue =latestValue; 
		            } 
		        } 
		        con.write(highestPaymentValue.getKey(), highestPaymentValue.getValue());
			}
	
		}
		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			// TODO Auto-generated method stub
			Configuration c= new Configuration();
			Job j=Job.getInstance(c,"Q3");
			j.setJarByClass(Q3.class);
			j.setMapperClass(MapForWordCount.class);
			j.setReducerClass(ReduceForWordCount.class);
			j.setOutputKeyClass(Text.class);
			j.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(j,new Path(args[0]));
			FileOutputFormat.setOutputPath(j,new Path(args[1]));
			System.exit(j.waitForCompletion(true)?0:1);
			
		}
}
