package reducer;

import org.apache.hadoop.io.Text;
import records.InversedIndexRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InversedIndexReducer
        extends org.apache.hadoop.mapreduce.Reducer<Text, InversedIndexRecord, Text, Text> {

    private final Text recordListText = new Text();

    @Override
    public void reduce(Text key, Iterable<InversedIndexRecord> values, Context context)
            throws IOException, InterruptedException {
        List<String> recordList = new ArrayList<String>();
        for (InversedIndexRecord currentRecord : values) {
            recordList.add((new InversedIndexRecord(currentRecord)).toString());
        }
        recordListText.set(recordList.toString());
        context.write(key, recordListText);
    }
}
