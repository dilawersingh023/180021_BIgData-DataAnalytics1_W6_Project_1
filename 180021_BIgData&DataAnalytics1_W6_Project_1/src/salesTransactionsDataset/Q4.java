package salesTransactionsDataset;
import java.io.IOException;

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

//Q.4 Find out the count of Product1 and Product2.                                                                                                                   (10 Marks)
//Here, it can analyze the consumption of Product 1 and Product 2. It can be found out that which product (either Product 1 or Product 2) is purchased by the most of the people.


public class Q4 {
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
				
					Text outputkey=new Text(words[1]);
					IntWritable outputvalue=new IntWritable(1);
					con.write(outputkey,outputvalue);
				
				}
			}
			
		}
		//reducer class
		public static class ReduceForWordCount extends Reducer<Text,IntWritable,Text,IntWritable>
		{
			public void reduce(Text word,Iterable<IntWritable> values,Context con)throws IOException, InterruptedException
			{
				Text ProductOne = new Text("Product1");
				Text ProductTwo = new Text("Product2");
				
				if(word.equals(ProductOne) || word.equals(ProductTwo)){

				int sum=0;
				for(IntWritable value:values)
				{
					sum=sum+value.get();
				}
				con.write(word,new IntWritable(sum));
				}
			}
			
		}
		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			// TODO Auto-generated method stub
			Configuration c= new Configuration();
			Job j=Job.getInstance(c,"Q4");
			j.setJarByClass(Q4.class);
			j.setMapperClass(MapForWordCount.class);
			j.setReducerClass(ReduceForWordCount.class);
			j.setOutputKeyClass(Text.class);
			j.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(j,new Path(args[0]));
			FileOutputFormat.setOutputPath(j,new Path(args[1]));
			System.exit(j.waitForCompletion(true)?0:1);
			
		}
}
