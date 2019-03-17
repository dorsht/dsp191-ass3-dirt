import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;

public class FilterMiFIleUsingTestSetFile {
	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

		private LinkedList<String> testSetPaths;

		@Override
		public void setup(Context context)  throws IOException {
			String testSetPath = context.getConfiguration().getStrings("test set")[0];
			testSetPaths = TestSetAux.getTestSetPaths(testSetPath);
			int i = 0;
		}

		@Override
		public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
			String [] splitted = line.toString().split("\t");
			String [] splittedSlash = splitted[0].split("/");
			String path = splittedSlash[0];
			if (testSetPaths.contains(path)){
				context.write(new Text(splitted[0]), new Text(splitted[1]));
			}
		}

		@Override
		public void cleanup(Context context) {
		}

	}


	public static class ReducerClass extends Reducer<Text,Text,Text, Text> {
		@Override
		public void setup(Context context) {
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
			for (Text value : values)
				context.write(key, value);
		}

		@Override
		public void cleanup(Context context)  throws IOException, InterruptedException {
		}
	}
}
