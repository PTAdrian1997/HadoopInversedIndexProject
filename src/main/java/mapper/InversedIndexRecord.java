package mapper;

public class InversedIndexRecord {

    private String filename = null;
    private Long lineNumber = null;
    private Long wordNumber = null;

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
}
