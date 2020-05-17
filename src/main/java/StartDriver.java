import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public class StartDriver {

    public static void main(String[] args) throws Exception {

        String stopWordsFile = args[0];

        Job job = Job.getInstance(new Configuration(), "inversed index");

        // add the stop-words file to be localize:
        job.addCacheFile(new Path(stopWordsFile).toUri());

        // Submit the job to the cluster:
        job.waitForCompletion(true);
    }

}
