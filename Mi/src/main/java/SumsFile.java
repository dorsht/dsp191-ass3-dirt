import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SumsFile {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private int state;

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            state = context.getConfiguration().getInt("index", -1);
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            if (state != -1) {
                String[] splitted = line.toString().split("\t");
                if (state == 0){
                    context.write(new Text(splitted[0]), new Text(splitted[3]));
                }
                else{
                    context.write(new Text(splitted[1]), new Text(splitted[3]));
                    context.write(new Text(splitted[2]), new Text(splitted[3]));
                }

            }
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }
    public static class ReducerClass extends Reducer<Text, Text,Text,Text> {
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void reduce(Text path, Iterable<Text> sums, Context context) throws IOException,
                InterruptedException {
                long totalSum = 0;
                for (Text sum : sums){
                    totalSum += Long.parseLong(sum.toString());
                }
                context.write(path, new Text(Long.toString(totalSum)));
            }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }
    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
}
