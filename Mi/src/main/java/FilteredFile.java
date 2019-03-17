import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.tartarus.snowball2.ext.englishStemmer;

import java.io.IOException;
import java.util.Arrays;


public class FilteredFile {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private String auxVerbs;

        private boolean startsWithLetter (String str){
            String tmp = str.toLowerCase();
            char c = tmp.charAt(0);
            return (c >='a' && c <= 'z');
        }

        private boolean isAuxVerb (String str){
            return auxVerbs.contains(str);
        }

        private String getProperty (String str, int idx){
            String[] splitted = str.split("/");
            return splitted[idx];
        }

        private String[] getTags(String[] biarc){
            String [] output = new String[biarc.length];
            for (int i = 0; i < biarc.length; i++){
                output[i] = getProperty(biarc[i], 1);
            }
            return output;
        }

        private boolean checkNoun (String toCheck){
            return (toCheck.startsWith("NN") || toCheck.startsWith("PRP"));
        }

        private int nounsPos (String[] tags){
            boolean first = checkNoun(tags[0]), last = checkNoun(tags[tags.length-1]);
            if (first && last) return 2;
            if (first) return 0;
            if (last) return 1;
            return -1;
        }

        private boolean headIsVerb (String[] biarc){
            String[] tags = getTags(biarc);
            for (int i = 0; i < tags.length; i++){
                if (getProperty (biarc[i], 3).equals("0")){
                    return (tags[i].startsWith("VB"));
                }
            }
            return false;
        }

        private int checkVerbPos (String[] tags){
            for (int i = 1; i < tags.length - 1; i++){
                if (tags[i].startsWith("VB")) return i;
            }
            return -1;
        }

        private boolean isLegalOrder (String[] biarc){
            String[] tags = getTags(biarc);
            switch (tags.length){
                case 0:
                case 1:
                case 2:
                    return false;
                case 3:
                    return ((nounsPos(tags) == 2) && startsWithLetter(biarc[0]) &&
                            startsWithLetter(biarc[2]) && tags[1].startsWith("VB"));
                default:
                    int biarcLength = biarc.length;
                    String[] newBiarc;
                    switch (nounsPos(tags)){
                        case -1:
                             newBiarc = Arrays.copyOfRange(biarc, 1, biarcLength - 1);
                            return isLegalOrder(newBiarc);
                        case 0:
                            newBiarc = Arrays.copyOfRange(biarc, 0, biarcLength - 1);
                            return isLegalOrder(newBiarc);
                        case 1:
                            newBiarc = Arrays.copyOfRange(biarc, 1, biarcLength);
                            return isLegalOrder(newBiarc);
                        case 2:
                            return (checkVerbPos(tags) != -1 && startsWithLetter(biarc[0]) &&
                                    startsWithLetter(biarc[biarcLength - 1]));
                        default:
                            return false;
                    }
            }
        }

        private int findFirstNoun (String[] tags){
            for (int i = 0; i < tags.length; i++){
                if (tags[i].startsWith("NN") || tags[i].startsWith("PRP")) return i;
            }
            return -1;
        }

        private int findLastNoun (String[] tags){
            for (int i = tags.length - 1; i >= 0; i--){
                if (tags[i].startsWith("NN") || tags[i].startsWith("PRP")) return i;
            }
            return -1;
        }

        private String createPathString (String[] pathArray){
            int pathArrayLength = pathArray.length;
            if (pathArrayLength == 1) return (getProperty(pathArray[0], 0));
            String output = "";
            for (int i = 0; i < pathArrayLength - 1; i++){
                output = output + getProperty(pathArray[i], 0) + " ";
            }
            return (output + getProperty(pathArray[pathArrayLength - 1], 0));
        }

        private String[] getSyntacticPath (String verb, String[] biarc) {
            if (biarc.length == 3) return (new String[]{verb, getProperty(biarc[0], 0),
                                                        getProperty(biarc[2], 0)});
            String[] tags = getTags(biarc);
            int firstNoun = findFirstNoun(tags), lastNoun = findLastNoun(tags);
            String[] pathArray = Arrays.copyOfRange(biarc, firstNoun + 1, lastNoun);
            return new String[]{createPathString(pathArray), getProperty(biarc[firstNoun], 0),
                                getProperty(biarc[lastNoun], 0)};
        }

        private int hasSubjAndObj (String[] biarc){
            String[] tags = getTags(biarc);
            int first = findFirstNoun(tags), second = findLastNoun(tags);
            String firstProp = getProperty(biarc[first], 2), secondProp = getProperty(biarc[second], 2);
            if (firstProp.contains("obj") && secondProp.contains("subj")) return 1; // x _ y
            else if (firstProp.contains("subj") && secondProp.contains("obj")) return 2; // y _ x
            return -1;
        }

        private int checkAllConds (String[] biarc, String hd){
            return (headIsVerb(biarc) && startsWithLetter(hd) && !isAuxVerb(hd) && isLegalOrder(biarc)) ?
                    hasSubjAndObj(biarc) : -1;
        }

        private String[] englishStem (String [] results){
            String [] output = new String[3];
            for (int i = 0; i < 3; i++) {
                String str = results[i];
                String[] splitted = str.split(" ");
                String curr = "";
                englishStemmer es = new englishStemmer();
                for (String word : splitted) {
                    es.setCurrent(word);
                    es.stem();
                    curr = curr + " " + es.getCurrent();
                }
                output[i] = curr.substring(1);
            }
            return output;
        }

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            this.auxVerbs = "am is are were be been have has had do does did";
        }


    //experience      that/IN/compl/3 patients/NNS/nsubj/3 experience/VB/ccomp/0      3092

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String [] ngram = (line.toString()).split("\t");
            String [] biarc = ngram[1].split(" ");
            int res;
            if ((res = checkAllConds(biarc, ngram[0])) != -1) {
                String[] result = englishStem(getSyntacticPath(ngram[0], biarc));
                if (res == 1){
                    context.write(new Text(result[0] + "\t" + result[1] + "$\t" + result[2] + "%"),
                            new Text(ngram[2]));
                }
                else if (res == 2){
                    context.write(new Text(result[0] + "\t" + result[1] + "%\t" + result[2] + "$"),
                            new Text(ngram[2]));
                }

            }
        }

        @Override
        public void cleanup(Context context)  throws IOException, InterruptedException {
        }
    }
    public static class ReducerClass extends Reducer<Text, Text,Text,Text> {
        private Counter N;
        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            N = context.getCounter(N_COUNTER.Counter.N_COUNTER);
        }

        @Override
        public void reduce(Text path, Iterable<Text> ngrams, Context context) throws IOException,
                InterruptedException {
            for (Text ngram : ngrams){
                context.write(path, ngram);
                N.increment(Long.parseLong(ngram.toString()));
            }
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
