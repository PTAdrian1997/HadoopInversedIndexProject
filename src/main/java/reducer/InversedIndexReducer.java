package reducer;

import mapper.InversedIndexRecord;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InversedIndexReducer
        extends org.apache.hadoop.mapreduce.Reducer<Text, InversedIndexRecord, Text, Text> {

    private Text recordListText = new Text();
    public void reduce(Text key, Iterable<InversedIndexRecord> values, Context context)
            throws IOException, InterruptedException {
        List<InversedIndexRecord> recordList = new ArrayList<InversedIndexRecord>();
        for (InversedIndexRecord currentRecord : values) {
            recordList.add(currentRecord);
        }
        recordListText.set(recordList.toString());
        context.write(key, recordListText);
    }
}
