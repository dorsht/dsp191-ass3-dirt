import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.TreeMap;

public class PathsByWordsMiFile {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {


        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String [] splitted = line.toString().split("\t");
            String [] splittedSlash = splitted[0].split("/");
            String path = splittedSlash[0];
            if (splittedSlash[1].equals("x")){
                context.write(new Text(splittedSlash[2]+"$"), new Text(path +
                        "$\t" + splitted[1]));
            }
            else{ // splittedSlash[1].equals("y")
                context.write(new Text(splittedSlash[2]+"%"), new Text(path +
                        "%\t" + splitted[1]));
            }
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }

    }


    public static class ReducerClass extends Reducer<Text,Text,Text, Text> {
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            String output = "";
            TreeMap<String, Double> pathCounts = new TreeMap<>();
            for (Text value : values){
                String [] splitted = value.toString().split("\t");
                if (pathCounts.containsKey(splitted[0])){
                    Double count = pathCounts.get(splitted[0]) + Double.parseDouble(splitted[1]);
                    pathCounts.put(splitted[0],  count);
                }
                else{
                    pathCounts.put(splitted[0], Double.parseDouble(splitted[1]));
                }

            }
            if (pathCounts.size() > 1) {
                for (String path : pathCounts.keySet()) {
                    output = output.concat(path + "/" + pathCounts.get(path) + "\t");
                }
                output = output.substring(0, output.length() - 1);
                context.write(key, new Text(output));
            }
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }
}
