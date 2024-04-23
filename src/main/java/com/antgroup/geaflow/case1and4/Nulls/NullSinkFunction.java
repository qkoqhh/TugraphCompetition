package com.antgroup.geaflow.case1and4.Nulls;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NullSinkFunction extends RichFunction implements SinkFunction<IVertex<Long,Double>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NullSinkFunction.class);
    @Override
    public void open(RuntimeContext runtimeContext) {
        LOGGER.info("NullSink open: fake");
    }

    @Override
    public void close() {
        LOGGER.info("NullSink close: fake");
    }


    @Override
    public void write(IVertex<Long, Double> longDoubleIVertex) throws Exception {
        LOGGER.info("NullSink write: nothing");
    }
}
