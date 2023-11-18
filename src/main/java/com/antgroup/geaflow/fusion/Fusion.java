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
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Fusion {
    private static final Logger LOGGER = LoggerFactory.getLogger(Fusion.class);
    public static final double EPS = 1E-8;
    public static String DATA_PWD = "D:\\work\\resources\\sf1\\snapshot\\";
    public static String OUTPUT_PWD = "./target/tmp/data/result/pagerank";

    static Map<Long, MutablePair<Double, Set<Long>>> case1Answer=new ConcurrentHashMap<>();
    static Map<Long, Map<Long, Integer> > transferIn = new ConcurrentHashMap<>(), transferOut = new ConcurrentHashMap<>();

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
            PWindowSource<IVertex<Pair<Long,VertexType>, VertexValue>> prVertices = pipelineTaskContext.buildSource(
                    new FusionVertexSource(DATA_PWD), AllWindow.getInstance()
            ).withParallelism(conf.getInteger(MyConfigKeys.SOURCE_PARALLELISM));

            PWindowSource<IEdge<Pair<Long,VertexType>, EdgeValue>> prEdges = pipelineTaskContext.buildSource(
                    new FusionEdgeSource(DATA_PWD), AllWindow.getInstance()
            ).withParallelism(conf.getInteger(MyConfigKeys.SOURCE_PARALLELISM));

            int iterationParallelism = conf.getInteger(MyConfigKeys.ITERATOR_PARALLELISM);
            GraphViewDesc graphViewDesc = GraphViewBuilder
                    .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                    .withShardNum(iterationParallelism)
                    .withBackend(IViewDesc.BackendType.Memory)
                    .build();
            PGraphWindow<Pair<Long, VertexType>, VertexValue, EdgeValue> graphWindow =
                    pipelineTaskContext.buildWindowStreamGraph(prVertices, prEdges, graphViewDesc);

            SinkFunction<IVertex<Pair<Long, VertexType>, VertexValue>> sink = new FusionSinkFunction();

            graphWindow.compute(new FusionAlgorithm(4))
                    .compute(iterationParallelism)
                    .getVertices()
                    .sink(sink)
                    .withParallelism(conf.getInteger(MyConfigKeys.SINK_PARALLELISM));

        });

        return pipeline.execute();
    }

    public static class FusionAlgorithm extends VertexCentricCompute<Pair<Long,VertexType>, VertexValue, EdgeValue, Object> {


        public FusionAlgorithm(long iterations) {
            super(iterations);
        }

        @Override
        public VertexCentricComputeFunction<Pair<Long,VertexType>, VertexValue, EdgeValue, Object> getComputeFunction() {
            return new FusionCentricComputeFunction();
        }

        @Override
        public VertexCentricCombineFunction<Object> getCombineFunction() {
            return null;
        }
    }

    public static class FusionCentricComputeFunction extends AbstractVcFunc<Pair<Long, VertexType>, VertexValue, EdgeValue, Object> {
        @Override
        public void compute(Pair<Long, VertexType> vertexKey, Iterator<Object> messageIterator) {
            long vertexId = vertexKey.getFirst();
            long iteration = context.getIterationId();
            VertexValue vv = context.vertex().get().getValue();

            if( vertexKey.getSecond() == VertexType.Account){
                // Account
                Map<Long, Integer> inMap = transferIn.get(vertexId), outMap = transferOut.get(vertexId);
                if (iteration == 1) {
                    // Case3
                    double outEdgeAmount=0D, inEdgeAmount=0D;

                    for (IEdge<Pair<Long, VertexType>, EdgeValue> e : context.edges().getEdges()) {
                        final long targetId = e.getTargetId().getFirst();
                        if (e.getDirect() == EdgeDirection.IN) {
                            // Case 3
                            inEdgeAmount += e.getValue().transferAmount;

                            // Case 2
                            if (targetId < vertexId) {
                                inMap.compute(targetId, (k, v) -> (v == null) ? 1 : (v + 1));
                            }

                            context.sendMessage(e.getTargetId(), vertexId);

                            // Case 1
                            if (vv.owner != -1) {
                                Double loan = FileInput.account2loan.get(targetId);
                                if (loan != null) {
                                    case1Answer.compute(vv.owner, (k, v) -> {
                                        if(v==null){
                                            return new MutablePair<>(loan, new HashSet<Long>(){{add(targetId);}});
                                        }
                                        if(v.getRight().add(targetId)){
                                            v.setLeft( v.getLeft()+ loan);
                                        }
                                        return v;
                                    });
                                }
                            }

                        } else {
                            // Case 3
                            outEdgeAmount += e.getValue().transferAmount;

                            // Case 2
                            if (targetId < vertexId) {
                                outMap.compute(targetId, (k, v) -> (v == null) ? 1 : (v + 1));
                            }

                        }
                    }


                    // Case 3
                    if(inEdgeAmount >EPS && outEdgeAmount > EPS){
                        vv.ret3 = inEdgeAmount / outEdgeAmount;
                    }
                } else if (iteration == 2) {
                    // Case 2
                    messageIterator.forEachRemaining(obj -> {
                        long succ= (Long) obj;
                        Pair<Long, VertexType> succKey= new Pair<>(succ,VertexType.Account);
                        Map<Long,Integer> out =  transferOut.get(succ), in = inMap;
                        int ret = 0;
                        if (in.size() < out.size()) {
                            for (Map.Entry<Long, Integer> entry:in.entrySet()) {
                                if(out.containsKey(entry.getKey())){
                                    int tmp = out.get(entry.getKey()) * entry.getValue();
                                    context.sendMessage(new Pair<>(entry.getKey(), VertexType.Account), tmp);
                                    ret += tmp;
                                }
                            }
                        }else{
                            for (Map.Entry<Long,Integer> entry: out.entrySet()){
                                if(in.containsKey(entry.getKey())){
                                    int tmp = in.get(entry.getKey())*entry.getValue();
                                    context.sendMessage(new Pair<>(entry.getKey(), VertexType.Account), tmp);
                                    ret += tmp;
                                }
                            }
                        }
                        context.sendMessage(succKey, ret);
                        vv.ret2 += ret;
                    });
                } else if (iteration == 3) {
                    // Case 2
                    messageIterator.forEachRemaining(obj -> {
                        Integer value = (Integer) obj;
                        vv.ret2 += value;
                    });
                }
            }else{
                // Person


                // Case 4
                if(iteration==1){// the first time of visiting current vertex
                    vv.guaranteeSet.add(vertexId);
                    if(FileInput.person2loan.containsKey(vertexId)) {
                        for (IEdge<Pair<Long, VertexType>, EdgeValue> inEdge : this.context.edges().getEdges()) {
                            this.context.sendMessage(inEdge.getTargetId(), vertexId);
                        }
                    }
                }else if(iteration<=4){
                    List<Long> msgList = new ArrayList<>();
                    while(messageIterator.hasNext()){
                        Long msg= (Long) messageIterator.next();
                        if (vv.guaranteeSet.add(msg)){
                            // TBD: can we compute ahead and convey this value?
                            double loan=FileInput.person2loan.get(msg);
                            vv.ret4 += loan;
                            msgList.add(msg);
                        }
                    }
                    if(iteration<=3){
                        for(IEdge<Pair<Long, VertexType>,EdgeValue> inEdge:this.context.edges().getEdges()){
                            // TBD: can we convey a list?
                            for (Long msg:msgList) {
                                this.context.sendMessage(inEdge.getTargetId(), msg);
                            }
                        }
                    }
                }
            }

        }
    }
}
