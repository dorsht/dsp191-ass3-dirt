import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import org.joda.time.LocalTime;

import java.io.IOException;
import java.util.LinkedList;

public class Main {

    //private static String outputString;

    private static String setPaths (Job job, String filePath) throws IOException {
        FileInputFormat.addInputPath(job, new Path(filePath));
        String path = job.getJobName() + LocalTime.now().toString().replace(':','-');
        FileOutputFormat.setOutputPath(job, new Path (path));
        return path;
    }

    @SuppressWarnings("Duplicates")
    private static String filterJobMaker (Job job, String filePath) throws IOException {
        job.setJarByClass(FilteredFile.class);
        job.setMapperClass(FilteredFile.MapperClass.class);
        job.setPartitionerClass(FilteredFile.PartitionerClass.class);
        job.setCombinerClass(FilteredFile.ReducerClass.class);
        job.setReducerClass(FilteredFile.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        //job.setInputFormatClass(TextInputFormat.class);
        return setPaths(job, filePath);
    }

    @SuppressWarnings("Duplicates")
    private static String sumsJobMaker(Job job, String filePath) throws IOException{
        job.setJarByClass(SumsFile.class);
        job.setMapperClass(SumsFile.MapperClass.class);
        job.setPartitionerClass(SumsFile.PartitionerClass.class);
        job.setCombinerClass(SumsFile.ReducerClass.class);
        job.setReducerClass(SumsFile.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        return setPaths(job, filePath);

    }

    @SuppressWarnings("Duplicates")
    private static String pathSlotJobMaker(Job job, String filePath) throws IOException {
        job.setJarByClass(PathSlotSumsFile.class);
        job.setMapperClass(PathSlotSumsFile.MapperClass.class);
        job.setPartitionerClass(PathSlotSumsFile.PartitionerClass.class);
        job.setCombinerClass(SumsFile.ReducerClass.class);
        job.setReducerClass(SumsFile.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        return setPaths(job, filePath);
    }

    @SuppressWarnings("Duplicates")
    private static String joinPathPathSlotJobMaker (Job job, String pathSumsPath, String pathSlotSumsPath)
            throws IOException {
        job.setJarByClass(JoinPathPathSlotFile.class);
        job.setReducerClass(JoinPathPathSlotFile.ReducerClass.class);
        job.setPartitionerClass(JoinPathPathSlotFile.PartitionerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(pathSumsPath),
                TextInputFormat.class, JoinPathPathSlotFile.MapperClassPath.class);
        MultipleInputs.addInputPath(job, new Path (pathSlotSumsPath),
                TextInputFormat.class, JoinPathPathSlotFile.MapperClassPathSlot.class);
        String path = job.getJobName() + LocalTime.now().toString().replace(':','-');
        FileOutputFormat.setOutputPath(job, new Path(path));
        return path;
    }

    private static String calcMiSlotJobMaker (Job job, String pathIntermediate, String slotSumsPath, String output)
            throws IOException {
        job.setJarByClass(MiFile.class);
        job.setReducerClass(MiFile.ReducerClass.class);
        job.setPartitionerClass(MiFile.PartitionerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(slotSumsPath),
                TextInputFormat.class, MiFile.MapperClassSlotSums.class);
        MultipleInputs.addInputPath(job, new Path (pathIntermediate),
                TextInputFormat.class, MiFile.MapperClassJoinedPath.class);
        String path = output + "/Mi";
        FileOutputFormat.setOutputPath(job, new Path (path));
        return path;
    }

    private static void deletePaths(LinkedList<String> paths) throws IOException {
        Configuration deleteConf = new Configuration();
        for (String path : paths) {
            Path pathToDelete = new Path(path);
            FileSystem hdfs = FileSystem.get(deleteConf);
            if (hdfs.exists(pathToDelete)) {
                hdfs.delete(pathToDelete, true); // true for recursive
            }
        }
    }


    @SuppressWarnings("Duplicates")
    public static void main(String[] args) throws IOException, InterruptedException {
        LinkedList<String> paths = new LinkedList<>();
        String input = args[1], output = args[2];
        //BasicConfigurator.configure();
        JobControl filterJC = new JobControl("filter input");
        JobControl countSumsJC = new JobControl("count sums");
        JobControl calcMiJC = new JobControl("calc mi");


        /** First stage **/
        // Filter
        Configuration filterConf = new Configuration();
        GenericOptionsParser gop = new GenericOptionsParser(filterConf, args);
        final Job filterJob = Job.getInstance(filterConf, "filter");
        ControlledJob cjFilter = new ControlledJob(filterJob, new LinkedList<ControlledJob>());
        String filterPath = filterJobMaker(filterJob, input);
        paths.add(filterPath);
        filterJC.addJob(cjFilter);


        Thread filterThread = new Thread(filterJC);
        filterThread.start();
        while(!filterJC.allFinished()){
            System.out.println("not finished yet - filter input");
            Thread.sleep(5000L);
        }
        System.out.println("after while");
        if (filterJC.getFailedJobList().isEmpty()){
            System.out.println("failed list is empty");
        }
        else{
            for(ControlledJob cj : filterJC.getFailedJobList())
            System.out.println(cj.getMessage());
        }
        filterJC.stop();

        /** Second stage **/
        // Get path sums
        Configuration pathSumsConf = new Configuration();
        pathSumsConf.setInt("index", 0);
        Job pathSumsJob = Job.getInstance(pathSumsConf, "pathSums");
        ControlledJob cjPathSums = new ControlledJob(pathSumsJob, new LinkedList<ControlledJob>());
        cjPathSums.addDependingJob(cjFilter);
        String pathSumsPath = sumsJobMaker(pathSumsJob, filterPath);
        paths.add(pathSumsPath);
        countSumsJC.addJob(cjPathSums);

        // Get SlotX & SlotsY sums
        Configuration slotsSumsConf = new Configuration();
        slotsSumsConf.setInt("index", 1);
        Job slotsSumsJob = Job.getInstance(slotsSumsConf, "slotsSums");
        ControlledJob cjSlotsSums = new ControlledJob(slotsSumsJob, new LinkedList<ControlledJob>());
        cjSlotsSums.addDependingJob(cjFilter);
        String slotsSumsPath = sumsJobMaker(slotsSumsJob, filterPath);
        paths.add(slotsSumsPath);
        countSumsJC.addJob(cjSlotsSums);


        // Get SlotX & SlotY and path sums
        Configuration pathSlotsSumsConf = new Configuration();
        Job pathSlotsSumsJob = Job.getInstance(pathSlotsSumsConf, "pathSlotsSums");
        ControlledJob cjPathSlotsSums = new ControlledJob(pathSlotsSumsJob, new LinkedList<ControlledJob>());
        cjPathSlotsSums.addDependingJob(cjFilter);
        String pathSlotsSumsPath = pathSlotJobMaker(pathSlotsSumsJob, filterPath);
        paths.add(pathSlotsSumsPath);
        countSumsJC.addJob(cjPathSlotsSums);

        Thread countSumsThread = new Thread(countSumsJC);
        countSumsThread.start();
        while(!countSumsJC.allFinished()){
            System.out.println("not finished yet - count sums");
            Thread.sleep(5000L);
        }
        countSumsJC.stop();

        /** Third stage **/

        Counters counters = filterJob.getCounters();
        Counter counter = counters.findCounter(N_COUNTER.Counter.N_COUNTER);
        long N = counter.getValue();

        // Intermediate mi calculation for slots
        Configuration joinPathPathSlotsConf = new Configuration();
        joinPathPathSlotsConf.setLong("N", N);
        Job joinPathPathSlotsJob = Job.getInstance(joinPathPathSlotsConf, "joinPathPathSlots");
        ControlledJob cjJoinPathPathSlots = new ControlledJob(joinPathPathSlotsJob, new LinkedList<ControlledJob>());
        cjJoinPathPathSlots.addDependingJob(cjPathSums);
        cjJoinPathPathSlots.addDependingJob(cjPathSlotsSums);
        String pathJoinPathPathSlots = joinPathPathSlotJobMaker(joinPathPathSlotsJob, pathSumsPath, pathSlotsSumsPath);
        paths.add(pathJoinPathPathSlots);
        calcMiJC.addJob(cjJoinPathPathSlots);


        // Mi calculation for the slots
        Configuration calcMiSlotsConf = new Configuration();
        Job calcMiSlotsJob = Job.getInstance(calcMiSlotsConf, "calcMiSlots");
        ControlledJob cjCalcMiSlots = new ControlledJob(calcMiSlotsJob, new LinkedList<ControlledJob>());
        cjCalcMiSlots.addDependingJob(cjSlotsSums);
        cjCalcMiSlots.addDependingJob(cjJoinPathPathSlots);
        calcMiSlotJobMaker(calcMiSlotsJob, pathJoinPathPathSlots, slotsSumsPath, output);
        calcMiJC.addJob(cjCalcMiSlots);


        Thread calcMiThread = new Thread(calcMiJC);
        calcMiThread.start();
        while(!calcMiJC.allFinished()){
            System.out.println("not finished yet - calc mi");
            Thread.sleep(5000L);
        }
        calcMiJC.stop();
        deletePaths(paths);

        System.exit(0);
    }
}