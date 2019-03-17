import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class GenericTextPartitioner {
	public static class PartitionerClass extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}

	}
}
