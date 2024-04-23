package com.antgroup.geaflow.case1and4.case1.PersonValue;

import com.antgroup.geaflow.Util.Util;
import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.graph.PGraphWindow;
import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.impl.AllWindow;
import com.antgroup.geaflow.case1and4.Normals.NormalSource;
import com.antgroup.geaflow.case1and4.case1.AccountLoan.AccountLoan;
import com.antgroup.geaflow.case1and4.case1.AccountTransfer.AccountTransfer;
import com.antgroup.geaflow.case1and4.case4.Case4ConfigKeys;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.example.function.AbstractVcFunc;
import com.antgroup.geaflow.example.function.FileSink;
import com.antgroup.geaflow.example.util.EnvironmentUtil;
import com.antgroup.geaflow.example.util.PipelineResultCollect;
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

public class PersonValue {
    private static final Logger LOGGER = LoggerFactory.getLogger(PersonValue.class);
    private static final double EPS=1E-8;
    public static String DATA_PWD = "./src/main/resources/snapshot/";
    public static String OUTPUT_PWD = "./target/tmp/data/result/case1";

    public static void main(String[] args) {

        if (args.length==2){
            DATA_PWD=args[0];
            OUTPUT_PWD=args[1];
        }
        DATA_PWD= Util.flushDir(DATA_PWD);
        OUTPUT_PWD= Util.flushDir(OUTPUT_PWD);


        Environment environment = EnvironmentUtil.loadEnvironment(new String[0]);
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
                    new NormalSource<>(DATA_PWD + "PersonOwnAccount.csv",
                            line -> {
                                String[] fields = line.split("\\|");
                                List<IVertex<Tuple<Long,Boolean>,Double>> retList=new ArrayList<>();
                                IVertex<Tuple<Long,Boolean>,Double> personVertex = new ValueVertex<>(
                                        new Tuple<>(Long.valueOf(fields[0]),true),
                                        0.0
                                );
                                IVertex<Tuple<Long,Boolean>,Double> accountVertex = new ValueVertex<>(
                                        new Tuple<>(Long.valueOf(fields[1]),false),
                                        0.0
                                );
                                retList.add(personVertex);
                                retList.add(accountVertex);
                                return retList;
                            }), AllWindow.getInstance()
            ).withParallelism(conf.getInteger(Case4ConfigKeys.SOURCE_PARALLELISM));

            PWindowSource<IEdge<Tuple<Long,Boolean>, Integer>> prEdges = pipelineTaskContext.buildSource(
                    new NormalSource<>(DATA_PWD + "PersonOwnAccount.csv",
                            line -> {
                                String[] fields = line.split("\\|");
                                IEdge<Tuple<Long,Boolean>,Integer> edge=new ValueEdge<>(
                                        new Tuple<>(Long.valueOf(fields[0]),true),
                                        new Tuple<>(Long.valueOf(fields[1]),false),
                                        1
                                );
                                return Collections.singleton(edge);
                            }
                    ), AllWindow.getInstance()
            ).withParallelism(conf.getInteger(Case4ConfigKeys.SOURCE_PARALLELISM));

            int iterationParallelism = conf.getInteger(Case4ConfigKeys.ITERATOR_PARALLELISM);
            GraphViewDesc graphViewDesc = GraphViewBuilder
                    .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                    .withShardNum(iterationParallelism)
                    .withBackend(IViewDesc.BackendType.Memory)
                    .build();
            PGraphWindow<Tuple<Long,Boolean>, Double, Integer> graphWindow =
                    pipelineTaskContext.buildWindowStreamGraph(prVertices, prEdges, graphViewDesc);

            SinkFunction<IVertex<Tuple<Long,Boolean>, Double>> sink = new PersonValueSinkFunction();

            graphWindow.compute(new PersonValue.PersonValueAlgorithm(1))
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
            if(!vertexId.getF1())return;
            Set<Long> accountSet = new HashSet<Long>();
            for(IEdge<Tuple<Long,Boolean>,Integer> edge: this.context.edges().getOutEdges()){
                Tuple<Long,Boolean> account=edge.getTargetId();
                accountSet.addAll(AccountTransfer.accountID2TransferPreds.getOrDefault(account.getF0(),Collections.emptyList()));
            }
            double sum=0.0;
            for(Long account: accountSet){
                sum+= AccountLoan.accountID2loanAmount.getOrDefault(account,0.0);
            }
            this.context.setNewVertexValue(sum);
        }
    }
}
