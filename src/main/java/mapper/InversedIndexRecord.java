package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class InversedIndexRecord implements Writable {

    private String filename;
    private Long lineNumber;
    private Long wordNumber;

    public InversedIndexRecord(){}

    public InversedIndexRecord(String filename, Long lineNumber, Long wordNumber){
        this.filename = filename;
        this.lineNumber = lineNumber;
        this.wordNumber = wordNumber;
    }

    public String getFilename(){
        return this.filename;
    }

    public Long getLineNumber() {
        return this.lineNumber;
    }

    public Long getWordNumber(){
        return this.wordNumber;
    }

    public String toString(){
        return "InversedIndexRecord[filename=" + this.filename +
                ", line_number=" + this.lineNumber +
                ", word_number=" + this.wordNumber + "]";
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeChars(this.filename);
        dataOutput.writeLong(this.lineNumber);
        dataOutput.writeLong(this.wordNumber);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.filename = dataInput.readLine();
        this.lineNumber = dataInput.readLong();
        this.wordNumber = dataInput.readLong();
    }

    public static InversedIndexRecord read(DataInput in) throws IOException {
        InversedIndexRecord newRecord = new InversedIndexRecord();
        newRecord.readFields(in);
        return newRecord;
    }

}
