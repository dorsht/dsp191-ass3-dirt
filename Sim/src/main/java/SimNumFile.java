import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;

public class SimNumFile {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private String testSetPath;
        private LinkedList<String> testSetPairs;

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            testSetPath = context.getConfiguration().getStrings("test set")[0];
            testSetPairs = TestSetAux.getTestSetPathPairs(testSetPath);
        }



        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
//            String[] splitted = line.toString().split("\t"); //splitted[0] = w%, splitted[1]: p1%/2.4//p2%/3.2//p3%/0.2 ...
//            String[] pathsMis = splitted[1].split("//"); // pathMis[i] = pi%/2.6

            String [] splitted = line.toString().split("\t");
//            String [] pathsMis = splitted[1].split("//");
            for (int i = 1; i < splitted.length; i ++){
                String [] first = splitted[i].split("/");
                double firstMi = Double.parseDouble(first[1]);
                for (int j = 1; j < splitted.length; j++){
                    if (i != j){
                        String [] second = splitted[j].split("/");
                        double misSum = Double.parseDouble(second[1]) + firstMi;
                        String pairToCheck = first[0].substring(0, first[0].length() - 1) + "/" +
                                second[0].substring(0, second[0].length() - 1);
                        if (testSetPairs.contains (pairToCheck) && TestSetAux.bothEndsWithSameChar(first[0], second[0])) {
                            context.write(new Text(first[0] + "/" + second[0]), new Text(Double.toString(misSum)));
                        }
                    }
                }
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
            double sum = 0.0;
//            System.out.println(key);
            for (Text value : values){
                sum += Double.parseDouble(value.toString());
            }
            context.write(key, new Text(Double.toString(sum)));
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }


    // We used the reducer from SimDenFile because It's do the same thing.

}
