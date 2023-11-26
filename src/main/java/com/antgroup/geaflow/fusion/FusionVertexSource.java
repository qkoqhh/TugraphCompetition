package com.antgroup.geaflow.fusion;

import com.antgroup.geaflow.Util.Util;
import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.io.SourceFunction;
import com.antgroup.geaflow.api.window.IWindow;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueVertex;
import com.google.protobuf.compiler.PluginProtos;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.util.Pair;
import org.codehaus.janino.Java;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;

public class FusionVertexSource extends RichFunction implements SourceFunction<IVertex<Pair<Long,VertexType>,VertexValue>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FusionVertexSource.class);
    protected final String filePath;
    protected transient RuntimeContext runtimeContext;


    public FusionVertexSource(String filePath) {
        this.filePath = filePath;
    }

    static List<String> readFileLines(String fileName) {
        try {
            List<String> lines = FileUtils.readLines(new File(fileName), Charset.defaultCharset());
            return lines;
        } catch (IOException e) {
            throw new RuntimeException("error in read resource file: " + fileName, e);
        }
    }

    List<String> linesLoan,linesAccount,linesPerson;
    void readFile(){
        linesLoan=readFileLines(filePath+"Loan.csv");
        linesAccount=readFileLines(filePath+"Account.csv");
        linesPerson=readFileLines(filePath+"Person.csv");
    }



    List<IVertex<Pair<Long,VertexType>, VertexValue>> record=new ArrayList<>();
    void readLoan(int parallel,int index){
        int size = linesLoan.size();
        int readPos = Math.max (1, size *index /parallel);
        int readEnd = Math.min(size*(index+1)/parallel,size);
        for (int i=readPos; i<readEnd; i++){
            String line= linesLoan.get(i);
            int first = line.indexOf('|');
            int second = line.indexOf('|',first+1);
            long loanId = Long.parseLong(line.substring(0,first));
            double amount = Double.parseDouble(line.substring(first+1,second));
            record.add(new ValueVertex<>(new Pair<>(loanId,VertexType.Loan),new VertexValue(amount)));
        }
    }

    void readAccount(int parallel,int index){
        int size = linesAccount.size();
        int readPos = Math.max (1, size *index /parallel);
        int readEnd = Math.min(size*(index+1)/parallel,size);
        for (int i=readPos; i<readEnd; i++){
            String line= linesAccount.get(i);
            int first = line.indexOf('|');
            long accountId = Long.parseLong(line.substring(0,first));
            record.add(new ValueVertex<>(new Pair<>(accountId,VertexType.Account),new VertexValue()));
        }
    }

    void readPerson(int parallel,int index){
        int size = linesPerson.size();
        int readPos = Math.max (1, size *index /parallel);
        int readEnd = Math.min(size*(index+1)/parallel,size);
        for (int i=readPos; i<readEnd; i++){
            String line= linesPerson.get(i);
            int first = line.indexOf('|');
            long personId = Long.parseLong(line.substring(0,first));
            record.add(new ValueVertex<>(new Pair<>(personId,VertexType.Person),new VertexValue()));
        }
    }

    int readPos, listSize;
    @Override
    public void init(int parallel, int index) {
//        LOGGER.info("Parallel {} index {}",parallel,index);

        readFile();
        readLoan(parallel, index);
        readAccount(parallel, index);
        readPerson(parallel, index);


        readPos=0;
        listSize=record.size();

    }

    @Override
    public boolean fetch(IWindow<IVertex<Pair<Long,VertexType>,VertexValue>> window, SourceContext<IVertex<Pair<Long,VertexType>,VertexValue>> ctx) throws Exception {
//        LOGGER.info("collection source fetch taskId:{}, batchId:{}",
//                runtimeContext.getTaskArgs().getTaskId(), window.windowId());
        while (readPos < listSize) {
            IVertex<Pair<Long, VertexType>, VertexValue> out = record.get(readPos);
            long windowsId = window.assignWindow(out);
            if (window.windowId() == windowsId) {
                ctx.collect(out);
                readPos++;
            } else {
                break;
            }
        }
        return readPos < listSize;
    }

    @Override
    public void open(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    @Override
    public void close() {

    }
}
