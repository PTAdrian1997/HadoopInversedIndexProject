package mapper;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

//TODO: use the InversedIndexRecord for the output value type;
public class InversedIndexMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    private File stopwordsFile;
    private Text word = new Text();

    public void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {
        // get the inputFilename:
        String inputFileName = ((FileSplit)context.getInputSplit()).getPath().getName();
        // get the line number:
        long lineNumber = offset.get();
        // split the words:
        String separators = "\"\',.()?![]#$*+-;:_/\\<>@%& ";
        StringTokenizer stringTokenizer = new StringTokenizer(text.toString(), separators);
        long wordNumber = 0l;
        while (stringTokenizer.hasMoreElements()){
            String currentWord = stringTokenizer.nextToken();
            wordNumber++;
            word.set(currentWord);
            context.write(word, new Text("[" + inputFileName + ", "
                    + lineNumber + ", " +
                    wordNumber + "]"));
        }
    }

}
