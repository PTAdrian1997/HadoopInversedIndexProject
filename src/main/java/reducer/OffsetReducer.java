package reducer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.io.Text;

import records.LineNumberRecord;
import records.OffsetRecord;

import java.io.IOException;
import java.util.ArrayList;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class is supposed to compute the line number field
 * based on the offset and the number of characters in the actual text;
 */
public class OffsetReducer
        extends org.apache.hadoop.mapreduce.Reducer<Text, OffsetRecord, Text, Text> {
    @Override
    public void reduce(Text filenameText, Iterable<OffsetRecord> values, Context context)
            throws IOException, InterruptedException {
        List<OffsetRecord> originalRecordList = new ArrayList<>();
        List<Integer> visitedIndexes = new ArrayList<>();

        long currentLineNumber = 0;
        long currentOffset = 0;

        // setup the Gson builder:
        GsonBuilder gsonBuilder = new GsonBuilder();
        Gson gson = gsonBuilder.create();
        for (OffsetRecord value : values) {
            //NOTA BENE: You need to create a new object by using the constructor;
            // if you don't do that, all the objects in the list will be the same;
            originalRecordList.add(new OffsetRecord(value));
        }
        while (visitedIndexes.size() != originalRecordList.size()) {
            // search for the element that has the current offset:
            int currentRecordIndex = -1;
            for (int i = 0; i < originalRecordList.size() && currentRecordIndex == -1; i++) {
                if (!visitedIndexes.contains(i) &&
                        originalRecordList.get(i).getOffset() == currentOffset)
                    currentRecordIndex = i;
            }
            currentLineNumber++;
            String currentLineString = originalRecordList.get(currentRecordIndex).getLineString();
            visitedIndexes.add(currentRecordIndex);
            // add the number of chars in the current line + 1 to the current offset:
            currentOffset += currentLineString.length() + 1;
            context.write(
                    new Text(),
                    new Text(
                            gson.toJson(
                                    new LineNumberRecord(
                                            filenameText.toString(),
                                            currentLineNumber,
                                            currentLineString
                                    ))
                    )
            );
        }
    }
}
