import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import org.joda.time.LocalTime;

import java.io.IOException;
import java.util.LinkedList;


public class Main {
    private static String setPaths (Job job, String filePath) throws IOException {
        FileInputFormat.addInputPath(job, new Path(filePath));
        String path = job.getJobName() + LocalTime.now().toString().replace(':','-');
        FileOutputFormat.setOutputPath(job, new Path (path));
        return path;
    }

    @SuppressWarnings("Duplicates")
    private static String filterMiFileUsingTestSetJobMaker(Job job, String pathFilter) throws IOException {
        job.setJarByClass(FilterMiFIleUsingTestSetFile.class);
        job.setMapperClass(FilterMiFIleUsingTestSetFile.MapperClass.class);
        job.setPartitionerClass(GenericTextPartitioner.PartitionerClass.class);
        //job.setCombinerClass(PathsByWordsMiFile.ReducerClass.class);
        job.setReducerClass(FilterMiFIleUsingTestSetFile.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        return setPaths(job, pathFilter);
    }

    @SuppressWarnings("Duplicates")
    private static String stageAJobMaker(Job job, String pathFilter) throws IOException {
        job.setJarByClass(PathsByWordsMiFile.class);
        job.setMapperClass(PathsByWordsMiFile.MapperClass.class);
        job.setPartitionerClass(GenericTextPartitioner.PartitionerClass.class);
        //job.setCombinerClass(PathsByWordsMiFile.ReducerClass.class);
        job.setReducerClass(PathsByWordsMiFile.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        return setPaths(job, pathFilter);
    }

    @SuppressWarnings("Duplicates")
    private static String simDenSlotJobMaker(Job job, String pathCalcMiSlot) throws IOException {
        job.setJarByClass(SimDenFile.class);
        job.setMapperClass(SimDenFile.MapperClass.class);
        job.setPartitionerClass(GenericTextPartitioner.PartitionerClass.class);
        //job.setCombinerClass(SimDenFile.ReducerClass.class);
        job.setReducerClass(SimDenFile.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setNumReduceTasks(1);
        return setPaths(job, pathCalcMiSlot);
    }

    @SuppressWarnings("Duplicates")
    private static String simNumSlotJobMaker(Job job, String pathCalcMiSlot) throws IOException {
        job.setJarByClass(SimNumFile.class);
        job.setMapperClass(SimNumFile.MapperClass.class);
        job.setPartitionerClass(GenericTextPartitioner.PartitionerClass.class);
        //job.setCombinerClass(SimDenFile.ReducerClass.class);
        job.setReducerClass(SimNumFile.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setNumReduceTasks(1);
        return setPaths(job, pathCalcMiSlot);
    }

    private static String simJobMaker (Job job, String denPath, String numPath) {
        job.setJarByClass(SimFile.class);
        job.setReducerClass(SimFile.ReducerClass.class);
        job.setPartitionerClass(GenericTextPartitioner.PartitionerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(denPath),
                TextInputFormat.class, SimFile.MapperClassPaths.class);
        MultipleInputs.addInputPath(job, new Path (numPath),
                TextInputFormat.class, SimFile.MapperClassPairs.class);
        String path = job.getJobName();
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, new Path(path));
        return path;

    }

    @SuppressWarnings("Duplicates")
    private static String pathsSimJobMaker(Job job, String pathSimSlots) throws IOException {
        job.setJarByClass(pathsSimFile.class);
        job.setMapperClass(pathsSimFile.MapperClass.class);
        job.setPartitionerClass(GenericTextPartitioner.PartitionerClass.class);
        //job.setCombinerClass(pathsSimFile.ReducerClass.class);
        job.setReducerClass(pathsSimFile.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
//        return setPaths(job, pathSimSlots);
        FileInputFormat.addInputPath(job, new Path(pathSimSlots));
        String path = job.getJobName();
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, new Path (path));
        return path;
    }

    @SuppressWarnings("Duplicates")
    private static String SortJobMaker(Job job, String pathSimCalc, String output) throws IOException {
        job.setJarByClass(SortFile.class);
        job.setMapperClass(SortFile.MapperClass.class);
        job.setPartitionerClass(GenericTextPartitioner.PartitionerClass.class);
        job.setSortComparatorClass(SimSortComparator.class);
        //job.setCombinerClass(pathsSimFile.ReducerClass.class);
        job.setReducerClass(SortFile.ReducerClass.class);
        job.setMapOutputKeyClass(SimComprable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(SimComprable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(pathSimCalc));
        String path = output + "/Similarity";
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

    @SuppressWarnings("duplicate")
    public static void main(String[] args) throws IOException, InterruptedException {
        BasicConfigurator.configure();
        String input = args[1], //todo change args[4] to args[1]
                output = args[2], testSet = args[3];
        LinkedList<String> toDelete = new LinkedList<>();
        JobControl filterMiJC = new JobControl("filter mi");
        JobControl intermediateCalcSimJC = new JobControl("intermediate calc sim");
        JobControl calcSimJC = new JobControl("calc sim");
        JobControl pathSimSortJc = new JobControl("paths sim and sort");

        /** Fourth stage **/

        //filter the big MI file into a smaller one based on the test set
        Configuration filterMiFileUsingTestSetConf = new Configuration();
        filterMiFileUsingTestSetConf.setStrings("test set", testSet);
        Job filterMiFileUsingTestSetJob = Job.getInstance(filterMiFileUsingTestSetConf,
                "filterMiFileUsingTestSet");
        ControlledJob cjFilterMiFIleUsingTestSet = new ControlledJob(filterMiFileUsingTestSetJob,
                new LinkedList<ControlledJob>());
        String filterMiFileUsingTestSetPath = filterMiFileUsingTestSetJobMaker(filterMiFileUsingTestSetJob, input);
        toDelete.add(filterMiFileUsingTestSetPath);
        filterMiJC.addJob(cjFilterMiFIleUsingTestSet);

        Thread filterMiThread = new Thread(filterMiJC);
        filterMiThread.start();
        while (!filterMiJC.allFinished()){
            System.out.println("not finished yet - filter mi");
            Thread.sleep(10000);
        }
        if (!filterMiJC.getFailedJobList().isEmpty()){
            for (ControlledJob harta : filterMiJC.getFailedJobList())
                System.out.println(harta.getMessage());
        }
        filterMiJC.stop();

        /** fifth stage */
        @SuppressWarnings("duplicate")
         //Stage a in the algorithm of dirt, for the X & Y slots
        Configuration pathsByWordsMiConf = new Configuration();
        Job pathsByWordsMiJob = Job.getInstance(pathsByWordsMiConf, "pathsByWordsMi");
        ControlledJob cjPathsByWordsMi = new ControlledJob(pathsByWordsMiJob, new LinkedList<ControlledJob>());
        String pathStageASlots = stageAJobMaker(pathsByWordsMiJob, filterMiFileUsingTestSetPath);
        toDelete.add(pathStageASlots);
        intermediateCalcSimJC.addJob(cjPathsByWordsMi);

        // Create Denominator for sim formula, slots X & Y
        Configuration simDenSlotsConf = new Configuration();
        Job simDenSlotsJob = Job.getInstance(simDenSlotsConf, "simDenSlots");
        ControlledJob cjSimDenSlots = new ControlledJob(simDenSlotsJob, new LinkedList<ControlledJob>());
        String pathSimDenSlots = simDenSlotJobMaker(simDenSlotsJob, filterMiFileUsingTestSetPath); //todo remove output
        toDelete.add(pathSimDenSlots);
        intermediateCalcSimJC.addJob(cjSimDenSlots);

        // Create Numerator for sim formula, slots X & Y
        Configuration simNumSlotsConf = new Configuration();
        simNumSlotsConf.setStrings("test set", testSet);
        Job simNumSlotsJob = Job.getInstance(simNumSlotsConf, "simNumSlots");
        ControlledJob cjSimNumSlots = new ControlledJob(simNumSlotsJob, new LinkedList<ControlledJob>());
        cjSimNumSlots.addDependingJob(cjPathsByWordsMi);
        String pathSimNumSlots = simNumSlotJobMaker(simNumSlotsJob, pathStageASlots);
        toDelete.add(pathSimNumSlots);
        intermediateCalcSimJC.addJob(cjSimNumSlots);

        Thread intermediateCalcSimThread = new Thread(intermediateCalcSimJC);
        intermediateCalcSimThread.start();
        while (!intermediateCalcSimJC.allFinished()){
            System.out.println("not finished yet - intermediate Calc Sim");
            Thread.sleep(10000);
        }
        if (!intermediateCalcSimJC.getFailedJobList().isEmpty()){
            for (ControlledJob harta : intermediateCalcSimJC.getFailedJobList())
                System.out.println(harta.getMessage());
        }
        intermediateCalcSimJC.stop();

        /** sixt stage */

        @SuppressWarnings("Duplicates")
        Configuration simSlotsConf = new Configuration();
        simSlotsConf.setStrings("test set", testSet);
        Job simSlotsJob = Job.getInstance(simSlotsConf, "simSlots");
        ControlledJob cjSimSlots = new ControlledJob(simSlotsJob, new LinkedList<ControlledJob>());
        cjSimSlots.addDependingJob(cjSimNumSlots);
        cjSimSlots.addDependingJob(cjSimDenSlots);
        String pathSimSlots = simJobMaker(simSlotsJob, pathSimDenSlots, pathSimNumSlots);
        toDelete.add(pathSimSlots);
        calcSimJC.addJob(cjSimSlots);


        Thread calcSimThread = new Thread(calcSimJC);
        calcSimThread.start();
        while (!calcSimJC.allFinished()){
            System.out.println("not finished yet - Calc Sim");
            Thread.sleep(10000);
        }
        if (!calcSimJC.getFailedJobList().isEmpty()){
            for (ControlledJob harta : calcSimJC.getFailedJobList())
                System.out.println(harta.getMessage());
        }
        calcSimJC.stop();


        /** seventh stage **/
        // Calculate the similarity for Path
        Configuration pathsSimConf = new Configuration();
        Job pathsSimJob = Job.getInstance(pathsSimConf, "pathsSim");
        ControlledJob cjPathsSim = new ControlledJob(pathsSimJob, new LinkedList<ControlledJob>());
        String pathsSimpath = pathsSimJobMaker(pathsSimJob, pathSimSlots);
        toDelete.add(pathsSimpath);
        pathSimSortJc.addJob(cjPathsSim);

        Configuration sortConf = new Configuration();
        Job sortJob = Job.getInstance(sortConf, "sort");
        ControlledJob cjSort = new ControlledJob(sortJob, new LinkedList<ControlledJob>());
        SortJobMaker(sortJob, pathsSimpath, output);
        cjSort.addDependingJob(cjPathsSim);
        pathSimSortJc.addJob(cjSort);


        Thread calcSimPathThread = new Thread(pathSimSortJc);
        calcSimPathThread.start();
        while (!pathSimSortJc.allFinished()){
            System.out.println("not finished yet - calc sim path & sort");
            Thread.sleep(10000);
        }
        pathSimSortJc.stop();

        deletePaths(toDelete);
        System.exit(0);
    }
}
