package com.antgroup.geaflow.fusion;

import com.antgroup.geaflow.Util.Util;
import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.io.SourceFunction;
import com.antgroup.geaflow.api.window.IWindow;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.google.protobuf.compiler.PluginProtos;
import com.influxdb.client.domain.File;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.util.Pair;
import org.codehaus.janino.Java;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    List<IVertex<Pair<Long,VertexType>,VertexValue>> vertexList;
    int readPos, listSize;

    @Override
    public void init(int parallel, int index) {
//        LOGGER.info("Parallel {} index {}",parallel,index);

        FileInput.readFile(filePath);
        FileInput.readLoan(parallel, index);
        FileInput.readPersonOwnAccount(parallel, index);
        FileInput.readLoanDepositAccount(parallel, index);
        FileInput.readPersonApplyLoan(parallel, index);
        FileInput.readAccountTransferAccount(parallel, index);
        FileInput.readPersonGuaranteePerson(parallel, index);

        FileInput.counterReadAccountTransferAccount.check();
        FileInput.counterReadPersonGuaranteePerson.check();

        vertexList=FileInput.vertexListArr.get(index);
        readPos=0;
        listSize=vertexList.size();

    }

    @Override
    public boolean fetch(IWindow<IVertex<Pair<Long,VertexType>,VertexValue>> window, SourceContext<IVertex<Pair<Long,VertexType>,VertexValue>> ctx) throws Exception {
//        LOGGER.info("collection source fetch taskId:{}, batchId:{}",
//                runtimeContext.getTaskArgs().getTaskId(), window.windowId());
        while (readPos < listSize) {
            IVertex<Pair<Long, VertexType>, VertexValue> out = vertexList.get(readPos);
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
