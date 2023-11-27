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
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Fusion {
    private static final Logger LOGGER = LoggerFactory.getLogger(Fusion.class);
    public static final double EPS = 1E-8;
    public static String DATA_PWD = "/home1/xqwang/project/sf1/snapshot/";
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
            PWindowSource<IVertex<Pair<Long,VertexType>, VertexValue>> prVertices = pipelineTaskContext.buildSource(
                    new FusionVertexSource(DATA_PWD), AllWindow.getInstance()
            ).withParallelism(conf.getInteger(MyConfigKeys.SOURCE_PARALLELISM));

            PWindowSource<IEdge<Pair<Long,VertexType>, Double>> prEdges = pipelineTaskContext.buildSource(
                    new FusionEdgeSource(DATA_PWD), AllWindow.getInstance()
            ).withParallelism(conf.getInteger(MyConfigKeys.SOURCE_PARALLELISM));

            int iterationParallelism = conf.getInteger(MyConfigKeys.ITERATOR_PARALLELISM);
            GraphViewDesc graphViewDesc = GraphViewBuilder
                    .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                    .withShardNum(iterationParallelism)
                    .withBackend(IViewDesc.BackendType.Memory)
                    .build();
            PGraphWindow<Pair<Long, VertexType>, VertexValue, Double> graphWindow =
                    pipelineTaskContext.buildWindowStreamGraph(prVertices, prEdges, graphViewDesc);

            SinkFunction<IVertex<Pair<Long, VertexType>, VertexValue>> sink = new FusionSinkFunction();

            graphWindow.compute(new FusionAlgorithm(5))
                    .compute(iterationParallelism)
                    .getVertices()
                    .sink(sink)
                    .withParallelism(conf.getInteger(MyConfigKeys.SINK_PARALLELISM));

        });

        return pipeline.execute();
    }

    public static class FusionAlgorithm extends VertexCentricCompute<Pair<Long,VertexType>, VertexValue, Double, Object> {


        public FusionAlgorithm(long iterations) {
            super(iterations);
        }

        @Override
        public VertexCentricComputeFunction<Pair<Long,VertexType>, VertexValue, Double, Object> getComputeFunction() {
            return new FusionCentricComputeFunction();
        }

        @Override
        public VertexCentricCombineFunction<Object> getCombineFunction() {
            return null;
        }
    }

    public static class FusionCentricComputeFunction extends AbstractVcFunc<Pair<Long, VertexType>, VertexValue, Double, Object> {
        @Override
        public void compute(Pair<Long, VertexType> vertexKey, Iterator<Object> messageIterator) {
            Long vertexId = vertexKey.getFirst();
            long iteration = context.getIterationId();
            VertexValue vv = context.vertex().get().getValue();

            if (vertexKey.getSecond() == VertexType.Loan) {
                // Loan
                for (IEdge<Pair<Long, VertexType>, Double> edge : context.edges().getEdges()) {
                    Pair<Long, VertexType> targetVertex = edge.getTargetId();
                    if (targetVertex.getSecond() == VertexType.Account) {
                        // Case 1
                        context.sendMessage(targetVertex, new Pair<>(vertexId, vv.amount));
                    } else {
                        // Case 4
                        context.sendMessage(targetVertex, vv.amount);
                    }
                }
            } else if (vertexKey.getSecond() == VertexType.Account) {
                // Account
                if (iteration == 1) {
                    double outEdgeAmount = 0D, inEdgeAmount = 0D;
                    List<Pair<Long,VertexType>> tmpList=new ArrayList<>();
                    vv.inMap = new HashMap<>();
                    vv.outArr = new ArrayList<>();
                    Long owner = null;

                    for (IEdge<Pair<Long, VertexType>, Double> e : context.edges().getEdges()) {
                        final long targetId = e.getTargetId().getFirst();
                        if (e.getDirect() == EdgeDirection.IN) {
                            if (e.getTargetId().getSecond() == VertexType.Person) {
                                // Case 1
                                owner = targetId;
                                continue;
                            }


                            // Case 3
                            inEdgeAmount += e.getValue();

                            // Case 2
                            if (targetId < vertexId) {
                                vv.inMap.compute(targetId, (k,v) -> (v==null)? 1: v+1);
                            }

                            tmpList.add(e.getTargetId());


                        } else {
                            // Case 3
                            outEdgeAmount += e.getValue();

                            // Case 2
                            if (targetId < vertexId) {
                                vv.outArr.add(targetId);
                            }

                        }
                    }

                    if (!tmpList.isEmpty()) {
                        if (vv.outArr.isEmpty()){
                            if (owner != null) {
                                for (Pair<Long, VertexType> e : tmpList) {
                                    context.sendMessage(e, owner);
                                }
                            }
                        }else {
                            // Case 2
                            vv.outArr.add(vertexId);
                            // Case 1
                            vv.outArr.add(owner != null ? owner : -1);
                            Object[] arr = vv.outArr.toArray();
                            for (Pair<Long, VertexType> e : tmpList) {
                                context.sendMessage(e, arr);
                            }
                        }
                    }


                    // Case 3
                    if (inEdgeAmount > EPS && outEdgeAmount > EPS) {
                        vv.ret3 = inEdgeAmount / outEdgeAmount;
                    }
                } else if (iteration == 2) {
                    List<Long> ownerList = new ArrayList<>();
                    List<Pair<Long, Double>> depositList = new ArrayList<>();
                    messageIterator.forEachRemaining(obj -> {
                        if (obj instanceof Long){
                            ownerList.add((Long) obj);
                        }else if (obj instanceof Object[]) {
                            // from Account Transfer Account
                            Object[] msg = (Object[]) obj;
                            int n = msg.length;
                            if ((Long)msg[n - 1] != -1) {
                                // Case 1
                                Long owner = (Long) msg[n - 1];
                                ownerList.add(owner);
                            }


                            // Case 2
                            Pair<Long, VertexType> succKey = new Pair<>((Long)msg[n - 2], VertexType.Account);
                            Map<Long, Integer> out = new HashMap<>(), in = vv.inMap;
                            for (int i = 0; i < n - 2; i++) {
                                out.compute((Long) msg[i], (k, v) -> (v == null) ? 1 : v + 1);
                            }
                            int ret = 0;
                            if (in.size() < out.size()) {
                                for (Map.Entry<Long, Integer> entry : in.entrySet()) {
                                    if (out.containsKey(entry.getKey())) {
                                        int tmp = out.get(entry.getKey()) * entry.getValue();
                                        context.sendMessage(new Pair<>(entry.getKey(), VertexType.Account), tmp);
                                        ret += tmp;
                                    }
                                }
                            } else {
                                for (Map.Entry<Long, Integer> entry : out.entrySet()) {
                                    if (in.containsKey(entry.getKey())) {
                                        int tmp = in.get(entry.getKey()) * entry.getValue();
                                        context.sendMessage(new Pair<>(entry.getKey(), VertexType.Account), tmp);
                                        ret += tmp;
                                    }
                                }
                            }
                            context.sendMessage(succKey, ret);
                            vv.ret2 += ret;
                        } else {
                            // from Account Deposit Loan
                            // Case 1
                            depositList.add((Pair<Long, Double>) obj);
                        }
                    });

                    // Case 1
                    if (!depositList.isEmpty()) {
                        Object[] uniqueDepositList = new HashSet<>(depositList).toArray();
                        HashSet<Long> ownerSet = new HashSet<>(ownerList);
                        for (Long owner : ownerSet) {
                            this.context.sendMessage(new Pair<>(owner, VertexType.Person), uniqueDepositList);
                        }
                    }
                } else {
                    // iteation > 2
                    // Case 2
                    messageIterator.forEachRemaining(obj -> {
                        Integer value = (Integer) obj;
                        vv.ret2 += value;
                    });
                }
            } else if (vertexKey.getSecond() == VertexType.Person) {
                // Person
                if (iteration == 1) {
                    // Case 4
                    vv.guaranteeSet = new HashSet<>();
                    vv.guaranteeSet.add(vertexId);
                } else if (iteration == 2) {
                    // Case 4
                    double sum = 0D;
                    while (messageIterator.hasNext()) {
                        double amount = (double) messageIterator.next();
                        sum += amount;
                    }
                    context.sendMessageToNeighbors(new Pair<>(vertexId, sum));
                } else if (iteration <= 5) {
                    List<Pair<Long, Double>> msgList = new ArrayList<>();
                    if (iteration == 3) {
                        Set<Long> loanSet = new HashSet<>();
                        messageIterator.forEachRemaining( obj-> {
                            if (obj instanceof Object[]) {
                                // Case 1
                                Object[] loanList = (Object[]) obj;
                                for (Object loanObj : loanList) {
                                    Pair<Long, Double> loan = (Pair<Long, Double>) loanObj;
                                    if (loanSet.add(loan.getFirst())) {
                                        vv.ret1 += loan.getSecond();
                                    }
                                }
                            } else {
                                // Case 4
                                Pair<Long, Double> msg = (Pair<Long, Double>) obj;
                                if (vv.guaranteeSet.add(msg.getKey())) {
                                    vv.ret4 += msg.getSecond();
                                    msgList.add(msg);
                                }
                            }
                        });

                    } else {
                        // Case 4
                        messageIterator.forEachRemaining(obj-> {
                            Object[] msg = (Object[]) obj;
                            int n=msg.length;
                            for (int i=0; i<n; i+=2) {
                                if (vv.guaranteeSet.add((Long) msg[i])) {
                                    vv.ret4 += (Double) msg[i+1];
                                    msgList.add(new Pair<>((Long) msg[i], (Double) msg[i+1]));
                                }
                            }
                        });
                    }
                    if (iteration <= 4 && !msgList.isEmpty()) {
                        List<Object> output=new ArrayList<>(msgList.size()*2);
                        // Case 4
                        for (Pair<Long, Double> msg : msgList) {
                            output.add(msg.getFirst());
                            output.add(msg.getSecond());
                        }
                        Object[] finalOutput = output.toArray();
                        context.sendMessageToNeighbors(finalOutput);
                    }
                }

            }

        }
    }


}
