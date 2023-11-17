package com.antgroup.geaflow.fusion;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.io.SourceFunction;
import com.antgroup.geaflow.api.window.IWindow;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class FusionEdgeSource extends RichFunction implements SourceFunction<IEdge<Long,EdgeValue>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FusionEdgeSource.class);
    protected final String filePath;
    protected transient RuntimeContext runtimeContext;

    public FusionEdgeSource(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void open(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    List<IEdge<Long,EdgeValue>> edgeList;
    int readPos, listSize;
    @Override
    public void init(int parallel, int index) {
        while(!FileInput.finishAccountTransferAccount.get());
        while(!FileInput.finishPersonGuaranteePerson.get());
        edgeList = FileInput.edgeListArr.get(index);
        readPos = 0;
        listSize = edgeList.size();
    }

    @Override
    public boolean fetch(IWindow<IEdge<Long, EdgeValue>> window, SourceContext<IEdge<Long, EdgeValue>> ctx) throws Exception {
        LOGGER.info("collection source fetch taskId:{}, batchId:{}",
                runtimeContext.getTaskArgs().getTaskId(), window.windowId());
        while (readPos < listSize) {
            IEdge<Long, EdgeValue> out = edgeList.get(readPos);
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
    public void close() {

    }
}
