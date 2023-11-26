package com.antgroup.geaflow.fusion;

import com.antgroup.geaflow.Util.PartionFileInput;
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



    PartionFileInput inputLoan, inputAccount, inputPerson;


    List<IVertex<Pair<Long,VertexType>, VertexValue>> record=new ArrayList<>();
    void readLoan(int parallel,int index) throws IOException {
        inputLoan = new PartionFileInput(filePath+"Loan.csv",parallel,index);
        while (inputLoan.nextLine()){
            Long loanId = inputLoan.nextLong();
            Double amount = inputLoan.nextDouble();
            record.add(new ValueVertex<>(new Pair<>(loanId,VertexType.Loan),new VertexValue(amount)));
        }
    }

    void readAccount(int parallel,int index) throws IOException {
        inputAccount = new PartionFileInput(filePath+"Account.csv",parallel,index);
        while(inputAccount.nextLine()) {
            long accountId = inputAccount.nextLong();
            record.add(new ValueVertex<>(new Pair<>(accountId,VertexType.Account),new VertexValue()));
        }
    }

    void readPerson(int parallel,int index) throws IOException {
        inputPerson = new PartionFileInput(filePath+"Person.csv",parallel,index);
        while(inputPerson.nextLine()){
            long personId = inputPerson.nextLong();
            record.add(new ValueVertex<>(new Pair<>(personId,VertexType.Person),new VertexValue()));
        }
    }

    int readPos, listSize;
    @Override
    public void init(int parallel, int index) {
//        LOGGER.info("Parallel {} index {}",parallel,index);

        try {
            readLoan(parallel, index);
            readAccount(parallel, index);
            readPerson(parallel, index);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


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
