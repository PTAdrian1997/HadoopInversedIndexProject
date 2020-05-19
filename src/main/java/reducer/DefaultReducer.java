package reducer;

import mapper.InversedIndexRecord;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.mapred.Reducer;

/**
 * This class simply writes what receives from the mapper;
 */
public class DefaultReducer
    extends org.apache.hadoop.mapreduce.Reducer<Text, InversedIndexRecord, Text, Text> {
    private Text recordText = new Text();
    public void reduce(Text text, Iterable<InversedIndexRecord> values,
                       Context context) throws IOException, InterruptedException {
        for(InversedIndexRecord currentRecord: values){
            recordText.set(currentRecord.toString());
            context.write(text, recordText);
        }
    }

    public void close() throws IOException {
    }

    public void configure(JobConf jobConf) {
    }
}
