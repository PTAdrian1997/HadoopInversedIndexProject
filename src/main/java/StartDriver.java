import mapper.InversedIndexMapper;
import mapper.OffsetMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;


import org.apache.hadoop.mapreduce.lib.chain.Chain;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import records.InversedIndexRecord;
import records.LineNumberRecord;
import records.OffsetRecord;
import reducer.DefaultReducer;
import reducer.InversedIndexReducer;
import reducer.OffsetReducer;

/**
 * This here is the class that starts everything (The Driver class).
 * 1st argument: the fully qualified path to the stop words file;
 * 2nd argument: the input fully qualified path;
 * 3rd argument: the fully qualified path to the output;
 */
public class StartDriver extends Configured implements Tool {

    public static final String processedString = "/processed";
    public static final String tempString = "/temp";

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new StartDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        JobControl jobControl = new JobControl("JobChain");

        //TODO: read the stopWordsFilePath as an argument;
        String stopWordsFilePath = "/inverted_index_stopwords.txt";
        String inputFilePath = args[0];
        String outputFilePath = args[1];

        // configure the job1:
        Configuration conf1 = getConf();
        Job job1 = Job.getInstance(conf1);
        job1.setJobName("OffsetJob");
        job1.setJarByClass(OffsetMapper.class);

        FileInputFormat.setInputPaths(job1, new Path(inputFilePath));
        FileOutputFormat.setOutputPath(job1, new Path(outputFilePath + tempString));

        job1.setMapperClass(OffsetMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(OffsetRecord.class);
//        ChainMapper.addMapper(
//                job1,
//                OffsetMapper.class,
//                LongWritable.class,
//                Text.class,
//                Text.class,
//                OffsetRecord.class,
//                new JobConf(false)
//        );
        job1.setMaxMapAttempts(1);

//        ChainReducer.setReducer(
//                job1,
//                OffsetReducer.class,
//                Text.class,
//                OffsetRecord.class,
//                Text.class,
//                LineNumberRecord.class,
//                new JobConf(false)
//        );
        job1.setReducerClass(OffsetReducer.class);
        job1.setMaxReduceAttempts(1);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        ControlledJob controlledJob1 = new ControlledJob(conf1);

        // add the stop-words file to be localized:
        job1.addCacheFile(new Path(stopWordsFilePath).toUri());

        Configuration conf2 = getConf();
        Job job2 = Job.getInstance(conf2, "WordJob");
        job2.setJarByClass(InversedIndexMapper.class);

        job2.setMapperClass(InversedIndexMapper.class);
        job2.setMaxMapAttempts(1);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(InversedIndexRecord.class);

        job2.setReducerClass(InversedIndexReducer.class);
        job2.setMaxReduceAttempts(1);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job2, new Path(outputFilePath + processedString));
        FileInputFormat.setInputPaths(job2, new Path(outputFilePath + tempString + "/part*"));
        ControlledJob controlledJob2 = new ControlledJob(conf2);

        controlledJob2.addDependingJob(controlledJob1);

        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);

        // Submit the job1 to the cluster:
        int result1 = job1.waitForCompletion(true) ? 0 : 1;

        // Submit the 2nd job to the cluster:

        if(result1 == 0){
            return job2.waitForCompletion(true) ? 0 : 1;
        }else return result1;
    }
}
