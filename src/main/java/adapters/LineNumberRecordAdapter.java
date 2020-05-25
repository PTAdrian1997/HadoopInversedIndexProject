package adapters;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import records.LineNumberRecord;

import java.io.IOException;

public class LineNumberRecordAdapter extends TypeAdapter<LineNumberRecord> {
    @Override
    public void write(JsonWriter jsonWriter, LineNumberRecord lineNumberRecord) throws IOException {
        jsonWriter.beginObject();
        jsonWriter.name("filename");
        jsonWriter.value(lineNumberRecord.getFilename());
        jsonWriter.name("lineNumber");
        jsonWriter.value(lineNumberRecord.getLineNumber());
        jsonWriter.name("actualLineContent");
        jsonWriter.value(lineNumberRecord.getActualLineContent());
        jsonWriter.endObject();
    }

    @Override
    public LineNumberRecord read(JsonReader jsonReader) throws IOException {
        LineNumberRecord lineNumberRecord = new LineNumberRecord();
        jsonReader.beginObject();
        String fieldName = null;
        while (jsonReader.hasNext()){
            JsonToken token = jsonReader.peek();
            if (token.equals(JsonToken.NAME)) {
                fieldName = jsonReader.nextName();
            }
            if("filename".equals(fieldName)){
                token = jsonReader.peek();
                lineNumberRecord.setFilename(jsonReader.nextString());
            }
            if("lineNumber".equals(fieldName)){
                token = jsonReader.peek();
                lineNumberRecord.setLineNumber(jsonReader.nextLong());
            }
            if("actualLineContent".equals(fieldName)){
                token = jsonReader.peek();
                lineNumberRecord.setActualLineContent(jsonReader.nextString());
            }
        }
        jsonReader.endObject();
        return lineNumberRecord;
    }
}
