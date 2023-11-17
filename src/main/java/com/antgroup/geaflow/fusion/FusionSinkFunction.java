package com.antgroup.geaflow.fusion;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.model.graph.vertex.IVertex;

public class FusionSinkFunction extends RichFunction implements SinkFunction<IVertex<Long, VertexValue>> {
    @Override
    public void open(RuntimeContext runtimeContext) {

    }

    @Override
    public void close() {

    }

    @Override
    public void write(IVertex<Long, VertexValue> longVertexValueIVertex) throws Exception {

    }
}
