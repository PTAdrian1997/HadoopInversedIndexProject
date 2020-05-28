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

    public InversedIndexRecord(){ }

    public InversedIndexRecord(InversedIndexRecord other){
        this.filename = other.filename;
        this.lineNumber = other.lineNumber;
    }

    public InversedIndexRecord(String filename, long lineNumber) {
        this.filename = filename;
        this.lineNumber = lineNumber;
    }

    @Override
    public String toString() {
        return "InversedIndexRecord[filename=" + this.filename +
                ", line_number=" + this.lineNumber
                + "]";
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.filename);
        dataOutput.writeLong(this.lineNumber);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.filename = dataInput.readUTF();
        this.lineNumber = dataInput.readLong();
    }

    public String getFilename() {
        return filename;
    }

    public long getLineNumber() {
        return lineNumber;
    }

}
