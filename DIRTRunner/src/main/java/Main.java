
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.log4j.BasicConfigurator;

import javax.sound.sampled.LineUnavailableException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Scanner;

import static org.apache.hadoop.ipc.RetryCache.waitForCompletion;

public class Main {

    private static String bucketName, keyName, serviceRole, jobFlowRole;
    private static HadoopJarStepConfig hadoopMiJarStepConfig, hadoopSimJarStepConfig;

    /** Assumption: The user used the userInfo file correctly - The first row is the bucket name, the second
     one is the queue name, the third one is the role arn, etc.
     **/
    private static void readUserInfo () throws IOException {
        File file = new File("./userInfo.txt");

        BufferedReader br = new BufferedReader(new FileReader(file));

        bucketName = br.readLine();
        keyName = br.readLine();
        serviceRole = br.readLine();
        jobFlowRole = br.readLine();

        br.close();
    }

    private static void printMenuOptions(){
        System.out.println("==================================================");
        System.out.println("Please select option:");
        System.out.println("\t0 - Exit");
        System.out.println("\t1 - 10 Files with positive predicate rules");
        System.out.println("\t2 - 10 Files with negative predicate rules");
        System.out.println("\t3 - 100 Files with positive predicate rules");
        System.out.println("\t4 - 100 Files with negative predicate rules");
        System.out.println("==================================================");
    }

    private static void handleUserChoice (int choice){
        switch (choice){
            case 0:
                System.exit(0);
            case 1: // 10 files with positive
                hadoopMiJarStepConfig = new HadoopJarStepConfig()
                        .withJar("s3n://" + bucketName + "/ASS3_DIRT-1.0.jar")
                        .withMainClass("Main")
                        .withArgs("s3n://vladi-dsp-191-ass3-bucket/biarcs",
                                "s3n://" + bucketName + "/dirt-algorithm-output/10pos");
                hadoopSimJarStepConfig = new HadoopJarStepConfig()
                        .withJar("s3n://" + bucketName +"/ASS3_DIRT_SIM-1.0.jar")
                        .withMainClass("Main")
                        .withArgs("s3n://" + bucketName + "/dirt-algorithm-output/10pos/Mi",
                                "s3n://" + bucketName + "/dirt-algorithm-output/10pos",
                                "posTestSet");
                break;
            case 2: // 10 files with negative
                hadoopMiJarStepConfig = new HadoopJarStepConfig()
                        .withJar("s3n://" + bucketName + "/ASS3_DIRT-1.0.jar")
                        .withMainClass("Main")
                        .withArgs("s3n://vladi-dsp-191-ass3-bucket/biarcs",
                                "s3n://" + bucketName + "/dirt-algorithm-output/10neg");

                hadoopSimJarStepConfig = new HadoopJarStepConfig()
                        .withJar("s3n://" + bucketName + "/ASS3_DIRT_SIM-1.0.jar")
                        .withMainClass("Main")
                        .withArgs("s3n://" + bucketName + "/dirt-algorithm-output/10neg/Mi",
                                "s3n://" + bucketName + "/dirt-algorithm-output/10neg",
                                "negTestSet");
                break;
            case 3: // 100 files with positive
                hadoopMiJarStepConfig = new HadoopJarStepConfig()
                        .withJar("s3n://" + bucketName + "/ASS3_DIRT-1.0.jar")
                        .withMainClass("Main")
                        .withArgs("s3n://dsp191/syntactic-ngram/biarcs",
                                "s3n://" + bucketName + "/dirt-algorithm-output/100pos");
                hadoopSimJarStepConfig = new HadoopJarStepConfig()
                        .withJar("s3n://" + bucketName + "/ASS3_DIRT_SIM-1.0.jar")
                        .withMainClass("Main")
                        .withArgs("s3n://" + bucketName + "/dirt-algorithm-output/100pos/Mi",
                                "s3n://" + bucketName + "/dirt-algorithm-output/100pos",
                                "posTestSet");
                break;
            case 4: // 100 files with negative
                hadoopMiJarStepConfig = new HadoopJarStepConfig()
                        .withJar("s3n://" + bucketName + "/ASS3_DIRT-1.0.jar")
                        .withMainClass("Main")
                        .withArgs("s3n://dsp191/syntactic-ngram/biarcs",
                                "s3n://" + bucketName + "/dirt-algorithm-output/100neg");
                hadoopSimJarStepConfig = new HadoopJarStepConfig()
                        .withJar("s3n://" + bucketName + "/ASS3_DIRT_SIM-1.0.jar")
                        .withMainClass("Main")
                        .withArgs("s3n://" + bucketName + "/dirt-algorithm-output/100neg/Mi",
                                "s3n://" + bucketName + "/dirt-algorithm-output/100neg",
                                "negTestSet");
                break;
            default:
                System.exit(1);
        }
    }


    public static void main(String[] args) throws LineUnavailableException, InterruptedException {

        try {
            readUserInfo();
        } catch (IOException e) {
            e.printStackTrace();
        }

        BasicConfigurator.configure();

        final AmazonElasticMapReduce emr = AmazonElasticMapReduceClient.builder()
                .withRegion(Regions.US_EAST_1)
                .build();

        // For debugging
        StepFactory stepFactory = new StepFactory();

        StepConfig enbDebug = new StepConfig()
                .withName("enable debugging")
                .withActionOnFailure("TERMINATE_CLUSTER")
                .withHadoopJarStep(stepFactory.newEnableDebuggingStep());

        Scanner reader = new Scanner(System.in);
        printMenuOptions();
        int choice = reader.nextInt();
        reader.close();

        handleUserChoice(choice);
        // first stage
        StepConfig miStepConfig = new StepConfig()
                .withName("calculate_dirt_mi")
                .withHadoopJarStep(hadoopMiJarStepConfig)
                .withActionOnFailure("TERMINATE_CLUSTER");

        // second stage
        StepConfig simStepConfig = new StepConfig()
                .withName("calculate_dirt_sim")
                .withHadoopJarStep(hadoopSimJarStepConfig)
                .withActionOnFailure("TERMINATE_CLUSTER");


        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(20)
                .withMasterInstanceType(InstanceType.M1Medium.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.6.0")
                .withEc2KeyName(keyName)
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        ScriptBootstrapActionConfig bootStrapScript = new ScriptBootstrapActionConfig()
                .withPath("s3://" + bucketName + "/bootStrapScript");

        BootstrapActionConfig bootStrap = new BootstrapActionConfig()
                .withName("Download test set")
                .withScriptBootstrapAction(bootStrapScript);

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Dirt Algorithm")
                .withInstances(instances)
                .withSteps(enbDebug, miStepConfig)
//                .withSteps(enbDebug, simStepConfig)
//                .withSteps(enbDebug, miStepConfig, simStepConfig)
                .withServiceRole(serviceRole)
                .withJobFlowRole(jobFlowRole)
                .withLogUri("s3n://" + bucketName + "/dirt-algorithm-logs/")
                .withReleaseLabel("emr-4.2.0")
                .withBootstrapActions(bootStrap);


        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
        System.out.print("Waiting for job completion...");

        LinkedList<String> runningStatesList = new LinkedList<>(Arrays.asList("STARTING", "RUNNING", "BOOTSTRAPPING"));

        while (runningStatesList.contains(emr.describeCluster(new DescribeClusterRequest()
                .withClusterId(jobFlowId)).getCluster().getStatus().getState())){
            System.out.print(".");
            Thread.sleep(20000);
        }

        System.out.println("\nJob Terminated.");

        emr.shutdown();
        Beep.tone(5000,100);
        Thread.sleep(2000);
        Beep.tone(400,1000,0.2);
    }

}
