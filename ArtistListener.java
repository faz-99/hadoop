import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import java.util.*;

public class artistuser {
    public static Map<Integer, Integer> createFrequencyMap(int[] array) {
        Map<Integer, Integer> frequencyMap = new HashMap<>();

        for (int item : array) {
            if (frequencyMap.get(item) != null) {
                frequencyMap.put(item, frequencyMap.get(item) + 1);
            } else {
                frequencyMap.put(item, 1);
            }

        }

        return frequencyMap;
    }

    public static List<Integer> getKeysSortedByValue(Map<Integer, Integer> map) {
        List<Map.Entry<Integer, Integer>> entryList = new ArrayList<>(map.entrySet());
        Collections.sort(entryList, new Comparator<Map.Entry<Integer, Integer>>() {
            @Override
            public int compare(Map.Entry<Integer, Integer> entry1, Map.Entry<Integer, Integer> entry2) {
                return entry2.getValue().compareTo(entry1.getValue());
            }
        });

        List<Integer> sortedKeys = new ArrayList<>();
        for (Map.Entry<Integer, Integer> entry : entryList) {
            sortedKeys.add(entry.getKey());
        }

        return sortedKeys;
    }

    public static class countMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] stringValues = line.split("\\s+");
            String artistID = stringValues[1];
            int userID = 0;
            if (!stringValues[0].equals("userID")) {
                userID = Integer.parseInt(stringValues[0]);
                context.write(new Text(artistID), new IntWritable(userID));
            }

        }
    }

    public static class countReducer
            extends Reducer<Text, IntWritable, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context)
                throws IOException, InterruptedException {
            int size = 0;

            for (IntWritable value : values) {
                size++;
            }
            int[] array = new int[size];
            int counter = 0;
            for (IntWritable value : values) {

                array[counter] = value.get();
                counter++;
            }
            for (int i = 0; i < array.length; i++) {
                System.out.println("Element at index " + i + ": " + array[i]);
            }
            Map<Integer, Integer> frequencyMap = createFrequencyMap(array);
            List<Integer> sortedKeys = getKeysSortedByValue(frequencyMap);
            String result = "";
            for (int keyval : sortedKeys) {
                result += keyval + " ";
            }
            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Artist Count <input path> <output path>");
            System.exit(-1);
        }
        // creating a new config
        Configuration conf = new Configuration();
        // creating new job using that config
        Job job = Job.getInstance(conf, "usercount");
        job.setJarByClass(artistuser.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(countMapper.class); // setting mapper class
        job.setReducerClass(countReducer.class);// setting reducer class

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
