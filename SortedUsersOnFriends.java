import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.*;                                                             



public class UserSortedFriends {
	
    private static Map<Text, IntWritable> countMap = new HashMap<>();

	public static class UserSortedFriendsMapper extends
			Mapper<LongWritable,Text , Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] stringValues = line.split("\\s+");
			String userID=stringValues[0];
			
		    context.write(new Text(userID), new IntWritable(1));
			
		}
	}

	public static class UserSortedFriendsReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context)
		// override
				throws IOException, InterruptedException {
			int noOfFriends = 0;
			for (IntWritable value : values) {
				noOfFriends = noOfFriends+value.get();
			}
			
            countMap.put(new Text(key), new IntWritable(noOfFriends));
		}
        
    	@Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

    		List<Map.Entry<Text, IntWritable>> entryList = new ArrayList<>(countMap.entrySet());

    		// Sort the List based on the values using a custom comparator
            Collections.sort(entryList, new Comparator<Map.Entry<Text, IntWritable>>() {
                @Override
                public int compare(Map.Entry<Text, IntWritable> entry1, Map.Entry<Text, IntWritable> entry2) {
                    return entry2.getValue().compareTo(entry1.getValue());
                }
            });

            // Print the sorted map
            for (Map.Entry<Text, IntWritable> entry : entryList) {
                context.write(new Text("User : " + entry.getKey() +" , Friends Count"), entry.getValue());
            }
             
        }
    	
	}
	
	


	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err
					.println("Usage:  UserSortedFriends <input path> <output path>");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, " UserSortedFriends");
		job.setJarByClass(UserSortedFriends.class);
		job.setJobName("User Sorted Friends");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(UserSortedFriendsMapper.class);
		job.setReducerClass(UserSortedFriendsReducer.class);
		job.setReducerClass(UserSortedFriendsReducer.class);
        job.setMapOutputKeyClass(Text.class); 
        job.setMapOutputValueClass(IntWritable.class); 
        job.setOutputKeyClass(Text.class); 
        
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
