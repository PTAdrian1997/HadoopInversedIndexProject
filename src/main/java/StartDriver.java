import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This here is the class that starts everything (The Driver class).
 * 1st argument: the fully qualified path to the stop words file;
 * 2nd argument: the input fully qualified path;
 * 3rd argument: the fully qualified path to the output;
 */
public class StartDriver {

    public static void main(String[] args) throws Exception {

        String stopWordsFilePath = args[0];
        String inputFilePath = args[1];
        String outputFilePath = args[2];

        // configure the Job:
        Job job = Job.getInstance(new Configuration(), "inversed index");
        job.setJarByClass(StartDriver.class);

        // add the stop-words file to be localize:
        job.addCacheFile(new Path(stopWordsFilePath).toUri());

        FileInputFormat.addInputPath(job, new Path(inputFilePath));
        FileOutputFormat.setOutputPath(job, new Path(outputFilePath));
        // Submit the job to the cluster:
        job.waitForCompletion(true);
    }

}
