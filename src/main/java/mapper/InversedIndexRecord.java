package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InversedIndexRecord implements Writable {

    private Text filename;
    private LongWritable lineNumber;
    private LongWritable wordNumber;

    public InversedIndexRecord(Text filename, LongWritable lineNumber, LongWritable wordNumber) {
        this.filename = filename;
        this.lineNumber = lineNumber;
        this.wordNumber = wordNumber;
    }

    public String toString() {
        return "InversedIndexRecord[filename=" + this.filename +
                ", line_number=" + this.lineNumber +
                ", word_number=" + this.wordNumber + "]";
    }

    public void write(DataOutput dataOutput) throws IOException {
        this.filename.write(dataOutput);
        this.lineNumber.write(dataOutput);
        this.wordNumber.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.filename.readFields(dataInput);
        this.lineNumber.readFields(dataInput);
        this.wordNumber.readFields(dataInput);
    }

}
