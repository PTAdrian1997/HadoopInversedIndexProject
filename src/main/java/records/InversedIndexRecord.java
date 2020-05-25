package records;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InversedIndexRecord implements Writable {

    private String filename;
    private long lineNumber;
    private long wordNumber;

    public InversedIndexRecord(){ }

    public InversedIndexRecord(String filename, long lineNumber, long wordNumber) {
        this.filename = filename;
        this.lineNumber = lineNumber;
        this.wordNumber = wordNumber;
    }

    @Override
    public String toString() {
        return "InversedIndexRecord[filename=" + this.filename +
                ", line_number=" + this.lineNumber +
                ", word_number=" + this.wordNumber + "]";
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.filename);
        dataOutput.writeLong(this.lineNumber);
        dataOutput.writeLong(this.wordNumber);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.filename = dataInput.readUTF();
        this.lineNumber = dataInput.readLong();
        this.wordNumber = dataInput.readLong();
    }

}
