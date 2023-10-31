package com.antgroup.geaflow.case1and4.case4.GuaranteeGraph;

import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.graph.PGraphWindow;
import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.impl.AllWindow;
import com.antgroup.geaflow.case1and4.Normals.NormalSource;
import com.antgroup.geaflow.case1and4.case4.Case4ConfigKeys;
import com.antgroup.geaflow.case1and4.case4.PersonLoan.PersonLoan;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.tuple.Tuple;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Math.min;

public class GuaranteeGraph {
    private static final Logger LOGGER = LoggerFactory.getLogger(GuaranteeGraph.class);
    private static final double EPS=1E-8;
    public static String DATA_PWD = "./src/main/resources/snapshot/";
    public static String OUTPUT_PWD = "./target/tmp/data/result/case4";

    public static void main(String[] args) {

        if (args.length==2){
            DATA_PWD=args[0];
            OUTPUT_PWD=args[1];
        }

        Environment environment = EnvironmentUtil.loadEnvironment(args);
        IPipelineResult result = GuaranteeGraph.submit(environment);
        PipelineResultCollect.get(result);
        environment.shutdown();
    }

    static IPipelineResult submit(Environment environment) {
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.put(FileSink.OUTPUT_DIR, OUTPUT_PWD);

        pipeline.submit(pipelineTaskContext -> {
            Configuration conf = pipelineTaskContext.getConfig();
            PWindowSource<IVertex<Long, VertexInfo>> prVertices = pipelineTaskContext.buildSource(
                    new NormalSource<>(DATA_PWD + "test_Person.csv",
                            line -> {
                                String[] fields = line.split("\\|");
                                IVertex<Long, VertexInfo> vertex = new ValueVertex<>(
                                        Long.valueOf(fields[0]),
                                        new VertexInfo()
                                );
                                return Collections.singleton(vertex);
                            }), AllWindow.getInstance()
            ).withParallelism(conf.getInteger(Case4ConfigKeys.SOURCE_PARALLELISM));

            PWindowSource<IEdge<Long, Integer>> prEdges = pipelineTaskContext.buildSource(
                    new NormalSource<>(DATA_PWD + "test_PersonGuaranteePerson.csv",
                            line -> {
                                String[] fields = line.split("\\|");
                                IEdge<Long, Integer> inEdge = new ValueEdge<>(
                                        Long.parseLong(fields[1]),
                                        Long.parseLong(fields[0]),
                                        1,
                                        EdgeDirection.IN
                                );
                                List<IEdge<Long,Integer>> retList = new ArrayList<>(1);
                                retList.add(inEdge);
                                return retList;
                            }
                    ), AllWindow.getInstance()
            ).withParallelism(conf.getInteger(Case4ConfigKeys.SOURCE_PARALLELISM));

            int iterationParallelism = conf.getInteger(Case4ConfigKeys.ITERATOR_PARALLELISM);
            GraphViewDesc graphViewDesc = GraphViewBuilder
                    .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                    .withShardNum(2)
                    .withBackend(IViewDesc.BackendType.Memory)
                    .build();
            PGraphWindow<Long, VertexInfo, Integer> graphWindow =
                    pipelineTaskContext.buildWindowStreamGraph(prVertices, prEdges, graphViewDesc);

            SinkFunction<IVertex<Long, VertexInfo>> sink = new GuaranteeGraphSinkFunction();

            graphWindow.compute(new GuaranteeGraph.GuaranteeGraphAlgorithm(4))
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

    public static class GuaranteeGraphAlgorithm extends VertexCentricCompute<Long, VertexInfo, Integer, Tuple<Long,Integer>> {

        public GuaranteeGraphAlgorithm(long iterations) {
            super(iterations);
        }

        @Override
        public VertexCentricComputeFunction<Long, VertexInfo, Integer, Tuple<Long,Integer>> getComputeFunction() {
            return new GuaranteeGraph.GuaranteeGraphCentricComputeFunction();
        }

        @Override
        public VertexCentricCombineFunction<Tuple<Long,Integer>> getCombineFunction() {
            return null;
        }
    }

    static public class GuaranteeGraphCentricComputeFunction extends AbstractVcFunc<Long, VertexInfo, Integer, Tuple<Long,Integer>> {

        @Override
        public void compute(Long vertexId, Iterator<Tuple<Long,Integer>> messageIterator) {
            IVertex<Long,VertexInfo> now=this.context.vertex().get();
            VertexInfo nowInfo=now.getValue();
            if(this.context.getIterationId()==1){// the first time of visiting current vertex
//                for(IEdge<Long,Integer> inEdge:this.context.edges().getInEdges()){
//                    this.context.sendMessage(inEdge.getTargetId(),new Tuple<>(now.getId(),1));
//                }
                for(IEdge<Long,Integer> inEdge:this.context.edges().getInEdges()){
                    this.context.sendMessage(inEdge.getTargetId(),new Tuple<>(inEdge.getSrcId(),inEdge.getValue()));
                }

            }
            else{
                while(messageIterator.hasNext()){
                    Tuple<Long,Integer> msg=messageIterator.next();
                    boolean spread=false;
                    int oldDist=nowInfo.minDistMap.getOrDefault(msg.getF0(),Integer.MAX_VALUE);
                    if(msg.getF1()<oldDist){ // Maybe a useful message
                        nowInfo.minDistMap.put(msg.getF0(),msg.getF1());
                        if(oldDist==Integer.MAX_VALUE) {
                            nowInfo.value += PersonLoan.personID2loanAmount.get(msg.getF0());
                        }
                        if(msg.getF1()<3) {
                            for(IEdge<Long,Integer> inEdge:this.context.edges().getInEdges()){
                                this.context.sendMessage(inEdge.getTargetId(),new Tuple<>(msg.getF0(),msg.getF1()+inEdge.getValue()));
                            }
                        }
                    }
                }
                this.context.setNewVertexValue(nowInfo);
            }
        }

    }
}
