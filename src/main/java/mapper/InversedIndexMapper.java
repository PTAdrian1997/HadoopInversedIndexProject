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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

//TODO: use the InversedIndexRecord for the output value type;
public class InversedIndexMapper
        extends Mapper<Object, Text, Text, InversedIndexRecord> {

    private final Text word = new Text();

    private List<String> readStopWords(){
        List<String> stopWords = new ArrayList<>();
        try{
            File stopwordsFile = new File("/inverted_index_stopwords.txt");
            // read the stopwords line by line:
            BufferedReader br = new BufferedReader(new FileReader(stopwordsFile));
            String line;
            while((line = br.readLine()) != null){
                stopWords.add(line);
            }
            br.close();
        }catch (Exception e){
            System.err.println("InversedIndexMapper.readStopWords: " + e);
        }
        return stopWords;
    }

    @Override
    public void map(Object key, Text lineNumberRecordText, Context context)
            throws IOException, InterruptedException {
        // read the list of stopwords:
        List<String> stopwordsList = readStopWords();
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
            // check if the word is in the stopwords list:
            wordNumber++;
            word.set(currentWord);
            context.write(word,
                    new InversedIndexRecord(
                            lineNumberRecord.getFilename(),
                            lineNumberRecord.getLineNumber(),
                            wordNumber)
            );
        }
    }

}
