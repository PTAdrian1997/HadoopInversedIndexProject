import mapper.OffsetMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import records.OffsetRecord;
import reducer.DefaultReducer;
import reducer.OffsetReducer;

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
        Job job = Job.getInstance(new Configuration(),
                "inversed index");
        job.setJarByClass(StartDriver.class);

        //TODO: explore the ChainMapper

//        job.setMapperClass(OffsetMapper.class);
        ChainMapper.addMapper(
                job,
                OffsetMapper.class,
                LongWritable.class,
                Text.class,
                Text.class,
                OffsetRecord.class,
                new JobConf(false)
        );
        job.setMapperClass(ChainMapper.class);
        job.setMaxMapAttempts(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OffsetRecord.class);

        job.setReducerClass(OffsetReducer.class);
        job.setMaxReduceAttempts(1);

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
