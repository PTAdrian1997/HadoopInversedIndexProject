package reducer;

import org.apache.hadoop.io.Text;
import records.OffsetRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class simply writes what receives from the mapper;
 */
public class DefaultReducer
    extends org.apache.hadoop.mapreduce.Reducer<Text, OffsetRecord, Text, Text> {

    public void reduce(Text text, Iterable<OffsetRecord> values,
                       Context context) throws IOException, InterruptedException {
        List<String> recordList = new ArrayList<String>();
        for(OffsetRecord currentRecord: values){
            recordList.add(currentRecord.toString());
        }
        context.write(text, new Text(recordList.toString()));
    }
}
