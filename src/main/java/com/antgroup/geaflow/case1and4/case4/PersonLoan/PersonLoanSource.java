package com.antgroup.geaflow.case1and4.case4.PersonLoan;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.io.SourceFunction;
import com.antgroup.geaflow.api.window.IWindow;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.*;

public class PersonLoanSource<OUT> extends RichFunction implements SourceFunction<OUT> { // Same as LoanAmountSource
    private static final Logger LOGGER = LoggerFactory.getLogger(PersonLoanSource.class);

    protected final String filePath;
    static protected Map<String, List<String>> lineMap = new HashMap<>();
    protected final PersonLoanSource.FileLineParser<OUT> parser;
    protected transient RuntimeContext runtimeContext;

    protected static Map<Long, Double> personID2loanAmount;

    public PersonLoanSource(String filePath, PersonLoanSource.FileLineParser<OUT> parser, Map<Long,Double> personID2loanAmount) {
        this.filePath = filePath;
        this.parser = parser;
        PersonLoanSource.personID2loanAmount =personID2loanAmount;
    }

    @Override
    public void open(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    protected int readPos,readEnd;
    protected List<String> lines;
    protected List<OUT> record;
    @Override
    public void init(int parallel, int index) {
        record = new ArrayList<>();
        LOGGER.info("Parallel {} index {}",parallel,index);
        synchronized (lineMap){
            if(!lineMap.containsKey(filePath)){
                LOGGER.info("LineMap::  Parallel "+parallel+"; Index "+index);
                lineMap.put(filePath, readFileLines(filePath));
            }
            lines = lineMap.get(filePath);
        }
        int size = lines.size();
        readPos = Math.max (1, size*index/parallel);
        readEnd = Math.min(size*(index+1)/parallel,size);
        LOGGER.info("Index : {}  Size : {}",index, readEnd-readPos);
        long start = System.currentTimeMillis();
        for (int i=readPos ; i<readEnd ; i++){
            record.addAll(parser.parse(lines.get(i), personID2loanAmount));
        }
        readPos=0;
        readEnd = record.size();
        LOGGER.info("Index : {}  Time : {} ",index ,System.currentTimeMillis() - start);
    }

    @Override
    public boolean fetch(IWindow<OUT> window, SourceFunction.SourceContext<OUT> ctx) throws Exception {
        LOGGER.info("collection source fetch taskId:{}, batchId:{}, start readPos {}, totalSize {}",
                runtimeContext.getTaskArgs().getTaskId(), window.windowId(), readPos, lineMap.size());
        while (readPos < readEnd) {
            OUT out = record.get(readPos);
            long windowId = window.assignWindow(out);
            if (window.windowId() == windowId) {
                ctx.collect(out);
                readPos++;
            } else {
                break;
            }
        }
        boolean result = false;
        if (readPos <  readEnd ){
            result = true;
        }
        LOGGER.info("collection source fetch batchId:{}, current readPos {}, result {}",
                window.windowId(), readPos, result);
        return result;
    }

    @Override
    public void close() {
    }

    private List<String> readFileLines(String filePath) {
        try {
            List<String> lines = FileUtils.readLines(new File(filePath), Charset.defaultCharset());
            return lines;
        } catch (IOException e) {
            throw new RuntimeException("error in read resource file: " + filePath, e);
        }
    }

    public interface FileLineParser<OUT> extends Serializable {
        Collection<OUT> parse(String line, Map<Long,Double> personID2loanAmount);
    }
}
