package com.antgroup.geaflow.fusion;

import com.antgroup.geaflow.Util.Util;
import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.graph.PGraphWindow;
import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.impl.AllWindow;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.dsl.udf.table.math.E;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.example.function.AbstractVcFunc;
import com.antgroup.geaflow.example.function.FileSink;
import com.antgroup.geaflow.example.util.EnvironmentUtil;
import com.antgroup.geaflow.example.util.PipelineResultCollect;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.pipeline.Pipeline;
import com.antgroup.geaflow.pipeline.PipelineFactory;
import com.antgroup.geaflow.view.GraphViewBuilder;
import com.antgroup.geaflow.view.IViewDesc;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class Fusion {
    private static final Logger LOGGER = LoggerFactory.getLogger(Fusion.class);
    private static final double EPS = 1E-8;
    public static String DATA_PWD = "D:\\work\\resources\\sf1\\snapshot\\";
    public static String OUTPUT_PWD = "./target/tmp/data/result/pagerank";

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        if (args.length == 2) {
            DATA_PWD = args[0];
            OUTPUT_PWD = args[1];
        }
        DATA_PWD= Util.flushDir(DATA_PWD);
        OUTPUT_PWD= Util.flushDir(OUTPUT_PWD);

        Environment environment = EnvironmentUtil.loadEnvironment(new String[0]);
        IPipelineResult result = Fusion.submit(environment);
        PipelineResultCollect.get(result);
        environment.shutdown();

        long endTime = System.currentTimeMillis();
        LOGGER.info("Total Time : {} s", (endTime - startTime) / 1000.0);
    }

    static IPipelineResult submit(Environment environment) {
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.put(FileSink.OUTPUT_DIR, OUTPUT_PWD);

        pipeline.submit(pipelineTaskContext -> {
            Configuration conf = pipelineTaskContext.getConfig();
            PWindowSource<IVertex<Long, VertexValue>> prVertices = pipelineTaskContext.buildSource(
                    new FusionVertexSource(DATA_PWD), AllWindow.getInstance()
            ).withParallelism(conf.getInteger(MyConfigKeys.SOURCE_PARALLELISM));

            PWindowSource<IEdge<Long, EdgeValue>> prEdges = pipelineTaskContext.buildSource(
                    new FusionEdgeSource(DATA_PWD), AllWindow.getInstance()
            ).withParallelism(conf.getInteger(MyConfigKeys.SOURCE_PARALLELISM));

            int iterationParallelism = conf.getInteger(MyConfigKeys.ITERATOR_PARALLELISM);
            GraphViewDesc graphViewDesc = GraphViewBuilder
                    .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                    .withShardNum(iterationParallelism)
                    .withBackend(IViewDesc.BackendType.Memory)
                    .build();
            PGraphWindow<Long, VertexValue, EdgeValue> graphWindow =
                    pipelineTaskContext.buildWindowStreamGraph(prVertices, prEdges, graphViewDesc);

            SinkFunction<IVertex<Long, VertexValue>> sink = new FusionSinkFunction();

            graphWindow.compute(new FusionAlgorithm(3))
                    .compute(iterationParallelism)
                    .getVertices()
                    .sink(sink)
//                    .sink(v->{
//                        if(v.getValue()>0) {
//                            LOGGER.info("result {}", v);
//                        }
//                    })
                    .withParallelism(conf.getInteger(MyConfigKeys.SINK_PARALLELISM));

        });

        return pipeline.execute();
    }

    public static class FusionAlgorithm extends VertexCentricCompute<Long, VertexValue, EdgeValue, MessageValue> {


        public FusionAlgorithm(long iterations) {
            super(iterations);
        }

        @Override
        public VertexCentricComputeFunction<Long, VertexValue, EdgeValue, MessageValue> getComputeFunction() {
            return new FusionCentricComputeFunction();
        }

        @Override
        public VertexCentricCombineFunction<MessageValue> getCombineFunction() {
            return null;
        }
    }

    public static class FusionCentricComputeFunction extends AbstractVcFunc<Long, VertexValue, EdgeValue, MessageValue> {
        @Override
        public void compute(Long vertexId, Iterator<MessageValue> messageIterator) {
            long iteration = context.getIterationId();
            VertexValue vv = context.vertex().get().getValue();
            if((vertexId&1) > 0){
                // Person
            }else{
                // Account
            }

        }
    }
}
