package mapper;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

public class InversedIndexMapper
        extends MapReduceBase
        implements Mapper<Object, Text, Text, InversedIndexRecord> {

//    public void map(Object key, Text value, Context context)
//    throws IOException, InterruptedException {
//        // get the filename:
//        String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
//        // split the words:
//        String separators = "\"\',.()?![]#$*+-;:_/\\<>@%& ";
//        StringTokenizer stringTokenizer = new StringTokenizer(value.toString(), separators);
//        while (stringTokenizer.hasMoreElements()){
//            String currentWord = stringTokenizer.nextToken();
//
//        }
//    }

    public void map(Object o, Text text, OutputCollector<Text, InversedIndexRecord> outputCollector, Reporter reporter) throws IOException {
        // get the filename:

    }

    public void close() throws IOException {

    }

    public void configure(JobConf jobConf) {
        // get the cached stop words file:
    }
}
