package mapper;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

public class InversedIndexMapper
        extends Mapper<Object, Text, Text, Text> {

    private File stopwordsFile;
    private Text word = new Text();
    private Text recordText = new Text();

    public void map(Object o, Text text, Context context) throws IOException, InterruptedException {
        // get the inputFilename:
        String inputFileName = ((FileSplit)context.getInputSplit()).getPath().getName();
        // get the line number:
        // split the words:
        String separators = "\"\',.()?![]#$*+-;:_/\\<>@%& ";
        StringTokenizer stringTokenizer = new StringTokenizer(text.toString(), separators);

        while (stringTokenizer.hasMoreElements()){
            String currentWord = stringTokenizer.nextToken();
            InversedIndexRecord currentRecord = new InversedIndexRecord(
                    inputFileName, 0L, 0L
            );
            word.set(currentWord);
            recordText.set(currentRecord.toString());
            context.write(word, recordText);
        }
    }

}
