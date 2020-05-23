package records;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LineNumberRecord implements Writable {

    private String filename;
    private String lineString;
    private long offset;

    public LineNumberRecord(){
    }

    public LineNumberRecord(String filename, String lineString, long offset) {
        this.filename = filename;
        this.lineString = lineString;
        this.offset = offset;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.filename);
        dataOutput.writeUTF(this.lineString);
        dataOutput.writeLong(this.offset);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.filename = dataInput.readUTF();
        this.lineString = dataInput.readUTF();
        this.offset = dataInput.readLong();
    }

    @Override
    public String toString(){
        return "LineNumberRecord[filename=" + this.filename +
                ", lineString=" + this.lineString +
                ", offset=" + this.offset +
                "]";
    }

}
