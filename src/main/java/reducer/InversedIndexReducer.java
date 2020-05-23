package reducer;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InversedIndexReducer
        extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {

    private Text recordListText = new Text();
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        List<String> recordList = new ArrayList<String>();
        for (Text currentRecord : values) {
            recordList.add(currentRecord.toString());
        }
        recordListText.set(recordList.toString());
        context.write(key, recordListText);
    }
}
