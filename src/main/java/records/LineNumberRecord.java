package records;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LineNumberRecord implements Writable {
    public void setLineNumber(long lineNumber) {
        this.lineNumber = lineNumber;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public void setActualLineContent(String actualLineContent) {
        this.actualLineContent = actualLineContent;
    }

    private long lineNumber;
    private String filename;
    private String actualLineContent;

    public LineNumberRecord(){}

    public LineNumberRecord(String filename, long lineNumber, String actualLineContent){
        this.lineNumber = lineNumber;
        this.filename = filename;
        this.actualLineContent = actualLineContent;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.filename);
        dataOutput.writeLong(this.lineNumber);
        dataOutput.writeUTF(this.actualLineContent);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.filename = dataInput.readUTF();
        this.lineNumber = dataInput.readLong();
        this.actualLineContent = dataInput.readUTF();
    }

    @Override
    public String toString(){
        return "LineNumberRecord[lineNumber=" + this.lineNumber +
                ", filename=" + this.filename +
                ", actualLineContent=" + this.actualLineContent +
                "]";
    }

    public long getLineNumber() {
        return lineNumber;
    }

    public String getFilename() {
        return filename;
    }

    public String getActualLineContent() {
        return this.actualLineContent;
    }
}
