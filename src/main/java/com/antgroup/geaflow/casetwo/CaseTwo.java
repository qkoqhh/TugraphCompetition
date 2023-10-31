package com.antgroup.geaflow.casetwo;

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
import com.antgroup.geaflow.example.function.FileSource;
import com.antgroup.geaflow.example.util.EnvironmentUtil;
import com.antgroup.geaflow.example.util.PipelineResultCollect;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.IDEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueVertex;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.pipeline.Pipeline;
import com.antgroup.geaflow.pipeline.PipelineFactory;
import com.antgroup.geaflow.view.GraphViewBuilder;
import com.antgroup.geaflow.view.IViewDesc;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.mapreduce.ID;
import org.codehaus.janino.Java;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CaseTwo {
    private static final Logger LOGGER = LoggerFactory.getLogger(CaseTwo.class);
    private static final double EPS = 1E-8;
    public static String DATA_PWD = "D:\\work\\resources\\sf1\\snapshot\\";
    public static String OUTPUT_PWD = "./target/tmp/data/result/pagerank";

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        if (args.length == 2) {
            DATA_PWD = args[0];
            OUTPUT_PWD = args[1];
        }

        Environment environment = EnvironmentUtil.loadEnvironment(args);
        IPipelineResult result = CaseTwo.submit(environment);
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
                    new MyFileSource<>(DATA_PWD + "Account.csv",
                            line -> {
                                String[] fileds = line.split("\\|");
                                IVertex<Long, VertexValue> vertex = new ValueVertex<>(
                                        Long.valueOf(fileds[0]),
                                        new VertexValue()
                                );
                                return Collections.singletonList(vertex);
                            }), AllWindow.getInstance()
            ).withParallelism(conf.getInteger(MyConfigKeys.SOURCE_PARALLELISM));

            PWindowSource<IEdge<Long, Object>> prEdges = pipelineTaskContext.buildSource(
                    new MyFileSource<>(DATA_PWD + "AccountTransferAccount.csv",
                            line -> {
                                String[] fields = line.split("\\|");
                                IDEdge<Long> outEdge = new IDEdge<>(
                                        Long.parseLong(fields[0]),
                                        Long.parseLong(fields[1])
                                );
                                IDEdge<Long> inEdge = new IDEdge<>(
                                        outEdge.getTargetId(),
                                        outEdge.getSrcId(),
                                        EdgeDirection.IN
                                );
                                List<IEdge<Long, Object>> retList = new ArrayList<>(2);
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
            PGraphWindow<Long, VertexValue, Object> graphWindow =
                    pipelineTaskContext.buildWindowStreamGraph(prVertices, prEdges, graphViewDesc);

            SinkFunction<IVertex<Long, VertexValue>> sink = new MySinkFunction();

            graphWindow.compute(new CaseTwoAlgorithm(3))
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

    public static class CaseTwoAlgorithm extends VertexCentricCompute<Long, VertexValue, Object, Object> {

        public CaseTwoAlgorithm(long iterations) {
            super(iterations);
        }

        @Override
        public VertexCentricComputeFunction<Long, VertexValue, Object, Object> getComputeFunction() {
            return new CaseTwoCentricComputeFunction();
        }

        @Override
        public VertexCentricCombineFunction<Object> getCombineFunction() {
            return null;
        }
    }

    public static class CaseTwoCentricComputeFunction extends AbstractVcFunc<Long, VertexValue, Object, Object> {
        @Override
        public void compute(Long vertexId, Iterator<Object> messageIterator) {
            long iteration = context.getIterationId();
            VertexValue vv = context.vertex().get().getValue();
            if (iteration == 1) {
                for (IEdge<Long, Object> e : context.edges().getEdges()) {
                    if (e.getDirect() == EdgeDirection.IN) {
                        if(vv.in.containsKey(e.getTargetId())){
                            vv.in.put(e.getTargetId(),vv.in.get(e.getTargetId())+1);
                        }else{
                            vv.in.put(e.getTargetId(),1);
                        }
                    } else {
                        if (vv.out.containsKey(e.getTargetId())){
                            vv.out.put(e.getTargetId(), vv.out.get(e.getTargetId())+1);
                        }else{
                            vv.out.put(e.getTargetId(),1);
                        }
                    }
                }
                for (IEdge<Long, Object> e : context.edges().getInEdges()) {
                    context.sendMessage(e.getTargetId(), vv.out);
                }
            } else if (iteration == 2) {
                Map<Long,Integer>mp=new HashMap<>();
                messageIterator.forEachRemaining(obj -> {
                    Map<Long,Integer> out = (Map<Long,Integer>) obj, in = vv.in;
                    if (in.size() < out.size()) {
                        in.forEach((k,v) -> {
                            if(out.containsKey(k)){
                                int _v=out.get(k);
                                if(mp.containsKey(k)){
                                    mp.put(k,mp.get(k)+v*_v);
                                }else{
                                    mp.put(k,v*_v);
                                }
                            }
                        });
                    }else{
                        out.forEach((k,v)->{
                            if(in.containsKey(k)){
                                int _v=in.get(k);
                                if(mp.containsKey(k)){
                                    mp.put(k,mp.get(k)+v*_v);
                                }else{
                                    mp.put(k,v*_v);
                                }
                            }
                        });
                    }
                });
                mp.forEach((k, v) -> {
                    context.sendMessage(k, v);
                });
            } else if (iteration == 3) {
                messageIterator.forEachRemaining(obj -> {
                    Integer value = (Integer) obj;
                    vv.ret += value;
                });
            }
        }
    }
}
