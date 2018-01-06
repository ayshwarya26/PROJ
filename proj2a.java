


import java.io.*;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.io.NullWritable;




public class Proj2a 
{
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split("\t");
	            if(str[4].equals("DATA ENGINEER"))
	            {
	            
	            //String jobandyear = str[4]+","+str[7];
	            
	            context.write(new Text(str[8]),new Text(str[7]));
	         }}
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	public static class CaderPartitioner extends
    Partitioner < Text, Text >
	   {
	      public int getPartition(Text key, Text value, int numReduceTasks)
	      {
	         String[] str = value.toString().split(",");
	         int year = Integer.parseInt(str[0]);


	         if(year==2011)
	         {
	            return 0 % numReduceTasks;
	         }
	         else if(year==2012)
	         {
	            return 1 % numReduceTasks ;
	         }
	         else if(year==2013)
	         {
	            return 2 % numReduceTasks ;
	         }
	         else if(year==2014)
	         {
	            return 3 % numReduceTasks ;
	         }
	         else if(year==2015)
	         {
	            return 4 % numReduceTasks ;
	         }
	         else
	         {
	            return 5 % numReduceTasks;
	         }
	      }
	   }

	
	  public static class ReduceClass extends Reducer<Text,Text,NullWritable,Text>
	   {
		    private LongWritable result = new LongWritable();
		    private TreeMap<Long,Text> repToRecordMap = new TreeMap<Long,Text>();
		    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		      long count=0;
		      String xx="";
		      String aa="";
		      
		         for (Text val : values)
		         {//String[] str = val.toString().split(",");
			      //String job = str[0];
			      //if(job=="DATA ENGINEER")
			      //{
		        	count++; 
		        	aa= val.toString();
			      //}
			      //key=new Text(job);
		         }
		        
		      //result.set(count);
		      xx= key.toString();
				//aa=values.toString();
				xx= xx + ','+count+','+aa;
				repToRecordMap.put(new Long(count), new Text(xx));
		      //repToRecordMap.put(key,result);
				if (repToRecordMap.size() > 1) {
				repToRecordMap.remove(repToRecordMap.firstKey()); //to remove 1st elements
				//repToRecordMap.remove(repToRecordMap.lastKey()); To remove last elements
						}
		      //context.write(key, result);
		      //context.write(key, new LongWritable(sum));
		         
		    }
		    protected void cleanup(Context context) throws IOException,
			InterruptedException {
			for (Text t : repToRecordMap.descendingMap().values()) {
				// Output our five records to the file system with a null key
			context.write(NullWritable.get(), t);
			}
			}
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job = Job.getInstance(conf, "Volume Count");
		    job.setJarByClass(Proj2a.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setPartitionerClass(CaderPartitioner.class);
		    job.setReducerClass(ReduceClass.class);
		    job.setNumReduceTasks(6);
		   
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(LongWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
