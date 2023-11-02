package com.antgroup.geaflow.case1and4.case1.PersonValue;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.IDVertex;

public class PersonValueSinkFunction extends RichFunction implements SinkFunction<IVertex<Long,Double>> {
    @Override
    public void open(RuntimeContext runtimeContext) {

    }

    @Override
    public void close() {

    }

    @Override
    public void write(IVertex<Long, Double> longDoubleIVertex) throws Exception {

    }
}
