package com.antgroup.geaflow.casethree;

import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.graph.PGraphWindow;
import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.impl.AllWindow;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.example.function.AbstractVcFunc;
import com.antgroup.geaflow.example.function.FileSink;
import com.antgroup.geaflow.example.util.EnvironmentUtil;
import com.antgroup.geaflow.example.util.PipelineResultCollect;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueVertex;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.pipeline.Pipeline;
import com.antgroup.geaflow.pipeline.PipelineFactory;
import com.antgroup.geaflow.view.GraphViewBuilder;
import com.antgroup.geaflow.view.IViewDesc;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CaseThree {
    private static final Logger LOGGER = LoggerFactory.getLogger(CaseThree.class);
    private static final double EPS=1E-8;
    public static String DATA_PWD = "D:\\work\\resources\\sf1\\snapshot\\";
    public static String OUTPUT_PWD = "./target/tmp/data/result/pagerank";

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        if (args.length==2){
            DATA_PWD=args[1];
            OUTPUT_PWD=args[2];
        }

        Environment environment = EnvironmentUtil.loadEnvironment(args);
        IPipelineResult result = CaseThree.submit(environment);
        PipelineResultCollect.get(result);
        environment.shutdown();

        long endTime = System.currentTimeMillis();
        LOGGER.info("Total Time : {} s",(endTime-startTime)/1000.0);
    }

    static IPipelineResult submit(Environment environment) {
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.put(FileSink.OUTPUT_DIR, OUTPUT_PWD);

        pipeline.submit(pipelineTaskContext -> {
            Configuration conf = pipelineTaskContext.getConfig();
            PWindowSource<IVertex<Long, Double>> prVertices = pipelineTaskContext.buildSource(
                    new MyFileSource<>(DATA_PWD + "Account.csv",
                            line -> {
                                String[] fileds = line.split("\\|");
                                IVertex<Long, Double> vertex = new ValueVertex<>(
                                        Long.valueOf(fileds[0]),
                                        -1D
                                );
                                return Collections.singleton(vertex);
                            }), AllWindow.getInstance()
            ).withParallelism(conf.getInteger(MyConfigKeys.SOURCE_PARALLELISM));

            PWindowSource<IEdge<Long, Double>> prEdges = pipelineTaskContext.buildSource(
                    new MyFileSource<>(DATA_PWD + "AccountTransferAccount.csv",
                            line -> {
                                String[] fields = line.split("\\|");
                                IEdge<Long, Double> outEdge = new ValueEdge<>(
                                        Long.parseLong(fields[0]),
                                        Long.parseLong(fields[1]),
                                        Double.parseDouble(fields[2])
                                );
                                IEdge<Long, Double> inEdge = new ValueEdge<>(
                                        outEdge.getTargetId(),
                                        outEdge.getSrcId(),
                                        outEdge.getValue(),
                                        EdgeDirection.IN
                                );
                                List<IEdge<Long,Double>> retList = new ArrayList<>(2);
                                retList.add(inEdge);
                                retList.add(outEdge);
                                return retList;
                            }
                    ), AllWindow.getInstance()
            ).withParallelism(conf.getInteger(MyConfigKeys.SOURCE_PARALLELISM));

            int iterationParallelism = conf.getInteger(MyConfigKeys.ITERATOR_PARALLELISM);
            GraphViewDesc graphViewDesc = GraphViewBuilder
                    .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                    .withShardNum(2)
                    .withBackend(IViewDesc.BackendType.Memory)
                    .build();
            PGraphWindow<Long, Double, Double> graphWindow =
                    pipelineTaskContext.buildWindowStreamGraph(prVertices, prEdges, graphViewDesc);

            SinkFunction<IVertex<Long, Double>> sink = new MySinkFunction();

            graphWindow.compute(new CaseThreeAlgorithm(1))
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

    public static class CaseThreeAlgorithm extends VertexCentricCompute<Long, Double, Double, Object> {

        public CaseThreeAlgorithm(long iterations) {
            super(iterations);
        }

        @Override
        public VertexCentricComputeFunction<Long, Double, Double, Object> getComputeFunction() {
            return new CaseThreeCentricComputeFunction();
        }

        @Override
        public VertexCentricCombineFunction<Object> getCombineFunction() {
            return null;
        }
    }

    static public class CaseThreeCentricComputeFunction extends AbstractVcFunc<Long, Double, Double, Object> {

        @Override
        public void compute(Long vertexId, Iterator<Object> messageIterator) {
            Double outEdgeAmount=0D, inEdgeAmount=0D;
            for(IEdge<Long,Double> e:context.edges().getOutEdges()){
                outEdgeAmount += e.getValue();
            }
            for(IEdge<Long,Double> e:context.edges().getInEdges()){
                inEdgeAmount += e.getValue();
            }

            if(inEdgeAmount < EPS || outEdgeAmount < EPS){
                return;
            }

            Double ret = inEdgeAmount / outEdgeAmount;
            context.setNewVertexValue(ret);
        }
    }
}
