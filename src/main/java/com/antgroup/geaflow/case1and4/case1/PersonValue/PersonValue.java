package com.antgroup.geaflow.case1and4.case1.PersonValue;

import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.graph.PGraphWindow;
import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.impl.AllWindow;
import com.antgroup.geaflow.case1and4.Normals.NormalSource;
import com.antgroup.geaflow.case1and4.case4.Case4ConfigKeys;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.example.function.AbstractVcFunc;
import com.antgroup.geaflow.example.function.FileSink;
import com.antgroup.geaflow.example.util.EnvironmentUtil;
import com.antgroup.geaflow.example.util.PipelineResultCollect;
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

public class PersonValue {
    private static final Logger LOGGER = LoggerFactory.getLogger(PersonValue.class);
    private static final double EPS=1E-8;
    public static String DATA_PWD = "./src/main/resources/snapshot/";
    public static String OUTPUT_PWD = "./target/tmp/data/result/case4";

    public static void main(String[] args) {

        if (args.length==2){
            DATA_PWD=args[0];
            OUTPUT_PWD=args[1];
        }

        Environment environment = EnvironmentUtil.loadEnvironment(args);
        IPipelineResult result = PersonValue.submit(environment);
        PipelineResultCollect.get(result);
        environment.shutdown();
    }

    static IPipelineResult submit(Environment environment) {
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.put(FileSink.OUTPUT_DIR, OUTPUT_PWD);

        pipeline.submit(pipelineTaskContext -> {
            Configuration conf = pipelineTaskContext.getConfig();
            PWindowSource<IVertex<Tuple<Long,Boolean>, Double>> prVertices = pipelineTaskContext.buildSource(
                    new NormalSource<>(DATA_PWD + "test_Person.csv",
                            line -> {

                            }), AllWindow.getInstance()
            ).withParallelism(conf.getInteger(Case4ConfigKeys.SOURCE_PARALLELISM));

            PWindowSource<IEdge<Tuple<Long,Boolean>, Integer>> prEdges = pipelineTaskContext.buildSource(
                    new NormalSource<>(DATA_PWD + "test_PersonGuaranteePerson.csv",
                            line -> {
                                String[] fields = line.split("\\|");

                            }
                    ), AllWindow.getInstance()
            ).withParallelism(conf.getInteger(Case4ConfigKeys.SOURCE_PARALLELISM));

            int iterationParallelism = conf.getInteger(Case4ConfigKeys.ITERATOR_PARALLELISM);
            GraphViewDesc graphViewDesc = GraphViewBuilder
                    .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                    .withShardNum(2)
                    .withBackend(IViewDesc.BackendType.Memory)
                    .build();
            PGraphWindow<Tuple<Long,Boolean>, Double, Integer> graphWindow =
                    pipelineTaskContext.buildWindowStreamGraph(prVertices, prEdges, graphViewDesc);

            SinkFunction<IVertex<Tuple<Long,Boolean>, Double>> sink = new PersonValueSinkFunction();

            graphWindow.compute(new PersonValue.PersonValueAlgorithm(4))
                    .compute(iterationParallelism)
                    .getVertices()
                    .sink(sink)
//                    .sink(v->{
//                        if(v.getValue()>0) {
//                            LOGGER.info("result {}", v);
//                        }
//                    })
                    .withParallelism(conf.getInteger(Case4ConfigKeys.SINK_PARALLELISM));

        });

        return pipeline.execute();
    }

    public static class PersonValueAlgorithm extends VertexCentricCompute<Tuple<Long,Boolean>, Double, Integer, Object> {

        public PersonValueAlgorithm(long iterations) {
            super(iterations);
        }

        @Override
        public VertexCentricComputeFunction<Tuple<Long, Boolean>, Double, Integer, Object> getComputeFunction() {
            return new PersonValue.PersonValueCentricComputeFunction();
        }

        @Override
        public VertexCentricCombineFunction<Object> getCombineFunction() {
            return null;
        }
    }

    static public class PersonValueCentricComputeFunction extends AbstractVcFunc<Tuple<Long,Boolean>, Double, Integer, Object> {

        @Override
        public void compute(Tuple<Long,Boolean> vertexId, Iterator<Object> messageIterator) {

        }


    }
}
