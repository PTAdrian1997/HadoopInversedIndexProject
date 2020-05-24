package tests;

import records.OffsetRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestRun {

    static class LocalPair{
        String line;
        long lineNumber;
    }

    public static void main(String[] args){
        int currentLineNumber = 0;
        long currentOffset = 0;
        List<OffsetRecord> originalRecordList = new ArrayList<>();

//        originalRecordList.add(new OffsetRecord("input1.txt", "test1 test3 test1", 54));
//        originalRecordList.add(new OffsetRecord("input1.txt", "test2 test1 test4 test5", 30));
//        originalRecordList.add(new OffsetRecord("input1.txt", "test1 test1 test2 test3 test2", 0));

        originalRecordList.add(new OffsetRecord("input2.txt", "test3", 36));
        originalRecordList.add(new OffsetRecord("input2.txt", "test4 test2 test3 test1", 12));
        originalRecordList.add(new OffsetRecord("input2.txt", "test3 test2", 0));
        List<LocalPair> orderedList = new ArrayList<>();
        while(!originalRecordList.isEmpty()){
            System.out.println(originalRecordList.toString());
            // search for the element that has the current offset:
            int currentRecordIndex = -1;
            for (int i = 0; i < originalRecordList.size() && currentRecordIndex == -1; i++) {
                if (originalRecordList.get(i).getOffset() == currentOffset) currentRecordIndex = i;
            }

            currentLineNumber++;
            String currentLineString = originalRecordList.get(currentRecordIndex).getLineString();
            // remove the current record from the original record list:
            originalRecordList.remove(currentRecordIndex);
            // add the number of chars in the current line + 1 to the current offset:
            currentOffset += currentLineString.length() + 1;
            LocalPair newLP = new LocalPair();
            newLP.line = currentLineString;
            newLP.lineNumber = currentLineNumber;
            orderedList.add(newLP);
        }

    }

}
