import mapper.InversedIndexMapper;
import mapper.LineNumberMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import records.LineNumberRecord;
import reducer.DefaultReducer;
import reducer.InversedIndexReducer;

/**
 * This here is the class that starts everything (The Driver class).
 * 1st argument: the fully qualified path to the stop words file;
 * 2nd argument: the input fully qualified path;
 * 3rd argument: the fully qualified path to the output;
 */
public class StartDriver {

    public static void main(String[] args) throws Exception {

        //TODO: read the stopWordsFilePath as an argument;
        String stopWordsFilePath = "/inverted_index_stopwords.txt";
        String inputFilePath = args[0];
        String outputFilePath = args[1];

        // configure the Job:
        Job job = Job.getInstance(new Configuration(), "inversed index");
        job.setJarByClass(StartDriver.class);

        //TODO: explore the ChainMapper

        job.setMapperClass(LineNumberMapper.class);
        job.setMaxMapAttempts(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LineNumberRecord.class);

        job.setReducerClass(DefaultReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // add the stop-words file to be localize:
        job.addCacheFile(new Path(stopWordsFilePath).toUri());

        FileInputFormat.addInputPath(job, new Path(inputFilePath));
        FileOutputFormat.setOutputPath(job, new Path(outputFilePath));
        // Submit the job to the cluster:
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
