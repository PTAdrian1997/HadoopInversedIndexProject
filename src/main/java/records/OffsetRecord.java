package records;

import org.apache.hadoop.io.*;
import reducer.OffsetReducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OffsetRecord implements Writable {

    private String filename;
    private String lineString;
    private long offset;

    public OffsetRecord(){
    }

    public OffsetRecord(String filename, String lineString, long offset) {
        this.filename = filename;
        this.lineString = lineString;
        this.offset = offset;
    }

    public OffsetRecord(OffsetRecord other){
        this.filename = other.getFilename();
        this.lineString = other.getLineString();
        this.offset = other.getOffset();
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
        return "OffsetRecord[filename=" + this.filename +
                ", lineString=" + this.lineString +
                ", offset=" + this.offset +
                ", charNumber=" + this.lineString.length() +
                "]";
    }

    public String getFilename() {
        return filename;
    }

    public String getLineString() {
        return lineString;
    }

    public long getOffset() {
        return offset;
    }
}
