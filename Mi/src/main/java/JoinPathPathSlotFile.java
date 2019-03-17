import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;

public class JoinPathPathSlotFile {
    public static class MapperClassPathSlot extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] splitted = line.toString().split("\t");
            context.write(new Text(splitted[0]), new Text(splitted[1] + "\t" + splitted[2]));
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

    }
    public static class MapperClassPath extends Mapper<LongWritable, Text,Text, Text> {

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] splitted = line.toString().split("\t");
            context.write(new Text(splitted[0]), new Text(splitted[1]));
        }


        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

    }


    public static class ReducerClass extends Reducer<Text,Text,Text, Text> {
        private long N;
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            N = context.getConfiguration().getLong("N", -1);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            LinkedList<String> Ws = new LinkedList<>();
            LinkedList<Long> sums = new LinkedList<>();
            long den = -1;
            for (Text value : values){
                String[] splitted = value.toString().split("\t");
                if (splitted.length == 1){
                    den = Long.parseLong(splitted[0]);
                }
                else{
                    Ws.addLast(splitted[0]);
                    sums.addLast(Long.parseLong(splitted[1]));
                }
            }
            if (den != -1 & Ws.size() > 1) {
                while (!Ws.isEmpty()) {
                    double result = (N * sums.removeFirst() * 1.0) / den;
                    context.write(new Text(Ws.removeFirst()), new Text(key.toString() + "\t" + result));
                }
            }
            Ws.clear();
            sums.clear();
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
