package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import records.LineNumberRecord;

import java.io.IOException;

/**
 * This mapper is supposed to map each file line to a record that contains the
 * following information:
 *  - the name of the file;
 *  - the actual line;
 */
public class LineNumberMapper
        extends Mapper<LongWritable, Text, Text, LineNumberRecord> {

    public void map(LongWritable offset, Text text, Context context)
            throws IOException, InterruptedException {
        // get the inputFilename:
        String inputFileName = ((FileSplit)context.getInputSplit()).getPath().getName();
        context.write(
                new Text(inputFileName),
                new LineNumberRecord(
                        inputFileName,
                        text.toString(),
                        offset.get()
                        )
                );
    }

}
