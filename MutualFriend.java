import java.io.*;
/*
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
*/
import java.util.*;
/*import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;*/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MutualFriend {

	
	//Mapper function
	public static class Map extends Mapper<LongWritable, Text, Text,Text>
	{
		private Text keypair = new Text();
		private Text listvalue = new Text();
		
		public void map(LongWritable key,Text val, Context con)throws IOException, InterruptedException
		{
			//split input on tab to get the words
			String[] record = val.toString().split("\t");
			//the first element of the record will be the user eg: Alice : Bob, Sam, Sara, Nancy user is Alice
			//String person = record[0];
			if(record.length == 2)
			{
				String person = record[0];
				//Create an arraylist of friends
				// without new ArrayList, type mismatch
				// record[1] contains the friends separated by ','
				//	Arrays.asList returns a fixed size list backed by the specified arrays
				//List<String> friends = (Arrays.asList(record[1].split(",")));
				List<String> friends =(Arrays.asList(record[1].split(",")));
				//for each friend in friends
				//for each friend in friends
				for(String friend : friends)
				{
					//String pair_of_friends = "";
					if(Integer.parseInt(person) < Integer.parseInt(friend))
							{
								keypair.set(person + "," +friend);
							}
					else
					{
						keypair.set(friend + "," + person);
					}
					// Create an array list that does not have the current friend. 
					// Initially copy the friend list to the array list and then remove the current friend.
					//ArrayList<String> list_sans_curr_friend =new ArrayList<String>(friends);
					//removing
					//list_sans_curr_friend.remove(friend);
					//Convert this arraylist to a string  since the type for list is Text
					//String sans_curr_frnd = String.join(",", list_sans_curr_friend);
					//set pair of friends  as key
					//keypair.set(pair_of_friends);
					//Set the list value
					//listvalue.set(sans_curr_frnd);
					//Write it to the context
					con.write(keypair, new Text((record[1])));
					
				}
				
				
			}//if
		}
	}// Mapper class

// Reducer class
public static class Reduce extends Reducer<Text,Text,Text,Text>
{
	private Text output =new Text();
	// Method to get common/mutual 
	/*public String getCommon(String str1, String str2, int idx)
	{
		// Create the map
		HashSet<String> map = new HashSet<String>();
		// Split both inputs on ','
		String[] s1 = str1.split(",");
		String[] s2 = str2.split(",");
		String output = "";
		for(String s:s1)
		{
			map.add(s);
		}
		// check if the same thing is there in string 2 and then add to the result
		for(String s:s2)
		{
		if(map.contains(s))
			output += (s + ",");
		}
		return output;
	}*/
	public void reduce(Text key, Iterable<Text> value, Context con) throws IOException, InterruptedException
	{
		//private Text output = new Text();
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		StringBuilder result = new StringBuilder();
		for(Text friends : value)
		{
			List<String> friendlist = Arrays.asList(friends.toString().split(","));
			for(String friend : friendlist)
			{
				if(map.containsKey(friend))
					result = result.append(friend + ",");
				else
					map.put(friend,1);
			}
		}
		//After appending all commas remove the last comma
		if(result.lastIndexOf(",") > -1)
		{
			result.deleteCharAt(result.lastIndexOf(","));
		}
		output.set(new Text(result.toString()));
		con.write(key, output);
		
	}
	
}// class


// Driver Program 
// Almost similar to Wordcount.java
@SuppressWarnings("deprecation")
public static void main(String args[])throws Exception
{
	Configuration conf = new Configuration();
  	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
  	// get all args
  	if (otherArgs.length != 2) {
  	
  	System.err.println("Usage: MutualFriends <inputfile hdfs path> <output file hdfs path>");
  	System.exit(2);
  	}
  	// create a job with name "Mutual Friends"
  	Job job = new Job(conf, "MutualFriends");
  	job.setJarByClass(MutualFriend.class);
  	job.setMapperClass(Map.class);
  	job.setReducerClass(Reduce.class);
  	// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
  	// set output key type
  	job.setOutputKeyClass(Text.class);
  	// set output value type
  	job.setOutputValueClass(Text.class);
  	//set the HDFS path of the input data
  	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
  	// set the HDFS path for the output
  	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
  	//Wait till job completion
  	System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}

