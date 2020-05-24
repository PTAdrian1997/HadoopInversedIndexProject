package records;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LineNumberRecord implements Writable {

    private long lineNumber;
    private String lineString;
    private String filename;

    public LineNumberRecord(){}

    public LineNumberRecord(String filename, long lineNumber, String lineString){
        this.lineString = lineString;
        this.lineNumber = lineNumber;
        this.filename = filename;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.filename);
        dataOutput.writeLong(this.lineNumber);
        dataOutput.writeUTF(this.lineString);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.filename = dataInput.readUTF();
        this.lineNumber = dataInput.readLong();
        this.lineString = dataInput.readUTF();
    }

    @Override
    public String toString(){
        return "LineNumberRecord[lineString='" + this.lineString +
                "', lineNumber=" + this.lineNumber +
                ", filename=" + this.filename +
                "]";
    }

    public long getLineNumber() {
        return lineNumber;
    }

    public String getLineString() {
        return lineString;
    }

    public String getFilename() {
        return filename;
    }
}
