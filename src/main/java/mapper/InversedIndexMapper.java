package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

public class InversedIndexMapper
        extends Mapper<Object, Text, Text, InversedIndexRecord> {

    private File stopwordsFile;
    private Text word = new Text();

    // the fields of the record:
    private Text currentRecordFilename = new Text();
    private LongWritable currentRecordLineNumber = new LongWritable();
    private LongWritable currentRecordWordNumber = new LongWritable();

    public void map(Object o, Text text, Context context) throws IOException, InterruptedException {
        // get the inputFilename:
        String inputFileName = ((FileSplit)context.getInputSplit()).getPath().getName();
        // get the line number:
        // split the words:
        String separators = "\"\',.()?![]#$*+-;:_/\\<>@%& ";
        StringTokenizer stringTokenizer = new StringTokenizer(text.toString(), separators);

        while (stringTokenizer.hasMoreElements()){
            String currentWord = stringTokenizer.nextToken();
            currentRecordFilename.set(inputFileName);
            //TODO: get the line number and occurrence number;
            currentRecordLineNumber.set(0L);
            currentRecordWordNumber.set(0L);
            InversedIndexRecord currentRecord = new InversedIndexRecord(
                    currentRecordFilename, currentRecordLineNumber, currentRecordWordNumber
            );
            word.set(currentWord);
            context.write(word, currentRecord);
        }
    }

}
