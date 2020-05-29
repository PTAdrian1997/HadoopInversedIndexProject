package mapper;

import adapters.LineNumberRecordAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import opennlp.tools.stemmer.PorterStemmer;
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
            File stopwordsFile = new File("inverted_index_stopwords.txt");
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
        System.out.println(stopwordsList.toString());
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(LineNumberRecord.class, new LineNumberRecordAdapter());
        Gson gson = gsonBuilder.create();
        LineNumberRecord lineNumberRecord = gson
                .fromJson(lineNumberRecordText.toString().trim(), LineNumberRecord.class);
        // split the words:
//        String separators = "\"\',.()?![]#$*+-;:_/\\<>@%& ";
//        StringTokenizer stringTokenizer = new StringTokenizer(
//                lineNumberRecord.getActualLineContent(), separators);
        String[] words = lineNumberRecord.getActualLineContent()
                .replaceAll("^[a-zA-Z0-9 ]", " ")
                .split(" ");
        long wordNumber = 0L;
        for(int wordIndex = 0; wordIndex < words.length; wordIndex++) {
            String currentWord = words[wordIndex];
            // get the root of the word:
//            String wordRoot = new PorterStemmer().stem(currentWord.trim());
            if(!stopwordsList.contains(currentWord)){
                // check if the word is in the stopwords list:
                wordNumber++;
                word.set(currentWord);
                context.write(word,
                        new InversedIndexRecord(
                                lineNumberRecord.getFilename(),
                                lineNumberRecord.getLineNumber())
                );
            }
        }
    }

}
