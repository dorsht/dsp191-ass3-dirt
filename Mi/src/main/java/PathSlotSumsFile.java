import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.IOException;

public class PathSlotSumsFile {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] splitted = line.toString().split("\t");
            context.write(new Text(splitted[0] + "\t" + splitted[1]),
                    new Text((splitted[3])));
            context.write(new Text(splitted[0] + "\t" + splitted[2]),
                    new Text((splitted[3])));
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }

    // We used the reducer from SumsFile because It's the same...

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
}
