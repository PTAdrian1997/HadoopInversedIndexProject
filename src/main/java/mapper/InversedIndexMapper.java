package mapper;

import adapters.LineNumberRecordAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import records.InversedIndexRecord;
import records.LineNumberRecord;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

//TODO: use the InversedIndexRecord for the output value type;
public class InversedIndexMapper
        extends Mapper<Object, Text, Text, Text> {

    private File stopwordsFile;
    private final Text word = new Text();

    @Override
    public void map(Object key, Text lineNumberRecordText, Context context)
            throws IOException, InterruptedException {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(LineNumberRecord.class, new LineNumberRecordAdapter());
        Gson gson = gsonBuilder.create();
        System.out.println(lineNumberRecordText.toString().trim());
        LineNumberRecord lineNumberRecord = gson
                .fromJson(lineNumberRecordText.toString().trim(), LineNumberRecord.class);
        // split the words:
        String separators = "\"\',.()?![]#$*+-;:_/\\<>@%& ";
        StringTokenizer stringTokenizer = new StringTokenizer(
                lineNumberRecord.getActualLineContent(), separators);
        long wordNumber = 0L;
        while (stringTokenizer.hasMoreElements()) {
            String currentWord = stringTokenizer.nextToken();
            wordNumber++;
            word.set(currentWord);
            context.write(word,
                    new Text(
                            gson.toJson(new InversedIndexRecord(
                                    lineNumberRecord.getFilename(),
                                    lineNumberRecord.getLineNumber(),
                                    wordNumber
                            ))
                    ));
        }
    }

}
