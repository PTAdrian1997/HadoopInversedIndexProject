package reducer;

import records.InversedIndexRecord;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.Text;
import records.LineNumberRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class simply writes what receives from the mapper;
 */
public class DefaultReducer
    extends org.apache.hadoop.mapreduce.Reducer<Text, LineNumberRecord, Text, Text> {

    public void reduce(Text text, Iterable<LineNumberRecord> values,
                       Context context) throws IOException, InterruptedException {
        List<String> recordList = new ArrayList<String>();
        for(LineNumberRecord currentRecord: values){
            recordList.add(currentRecord.toString());
        }
        context.write(text, new Text(recordList.toString()));
    }
}
