package reducer;

import mapper.InversedIndexRecord;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InversedIndexReducer
        extends org.apache.hadoop.mapreduce.Reducer<Text, InversedIndexRecord, Text, ArrayWritable> {
    private ArrayWritable recordArrayWritable = new ArrayWritable(InversedIndexRecord.class);

    public void reduce(Text key, Iterable<InversedIndexRecord> values, Context context)
            throws IOException, InterruptedException {
        List<InversedIndexRecord> recordList = new ArrayList<InversedIndexRecord>();
        for (InversedIndexRecord currentRecord : values) {
            recordList.add(currentRecord);
        }
        recordArrayWritable.set(recordList.toArray(new InversedIndexRecord[0]));
        context.write(key, recordArrayWritable);
    }
}
