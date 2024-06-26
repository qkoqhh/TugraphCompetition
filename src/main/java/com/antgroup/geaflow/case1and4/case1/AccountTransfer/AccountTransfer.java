package com.antgroup.geaflow.case1and4.case1.AccountTransfer;

import com.antgroup.geaflow.Util.Util;
import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.graph.PGraphWindow;
import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.impl.AllWindow;
import com.antgroup.geaflow.case1and4.Nulls.NullSinkFunction;
import com.antgroup.geaflow.case1and4.Nulls.NullSource;
import com.antgroup.geaflow.case1and4.case1.AccountLoan.AccountLoan;
import com.antgroup.geaflow.case1and4.case1.AccountLoan.AccountLoanSource;
import com.antgroup.geaflow.case1and4.case1.Case1ConfigKeys;
import com.antgroup.geaflow.case1and4.case4.Case4ConfigKeys;
import com.antgroup.geaflow.case1and4.case4.LoanAmount.LoanAmount;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.example.function.AbstractVcFunc;
import com.antgroup.geaflow.example.function.FileSink;
import com.antgroup.geaflow.example.util.EnvironmentUtil;
import com.antgroup.geaflow.example.util.PipelineResultCollect;
import com.antgroup.geaflow.example.util.ResultValidator;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.pipeline.Pipeline;
import com.antgroup.geaflow.pipeline.PipelineFactory;
import com.antgroup.geaflow.pipeline.task.PipelineTask;
import com.antgroup.geaflow.view.GraphViewBuilder;
import com.antgroup.geaflow.view.IViewDesc;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class AccountTransfer {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccountLoan.class);

    public static String DATA_PWD = "./src/main/resources/snapshot/";
    public static String RESULT_FILE_PATH = "./target/tmp/data/result/AccountTransfer";
//    public static final String RESULT_FILE_PATH = "C:\\Users\\qkoqhh\\IdeaProjects\\TuGraphPageRank\\target\\pagerank";

    public static Map<Long, List<Long>> accountID2TransferPreds = null;


    public static void main(String[] args) {
        if(args.length==2){
            DATA_PWD=args[0];
            RESULT_FILE_PATH=args[1];
        }
        DATA_PWD= Util.flushDir(DATA_PWD);
        RESULT_FILE_PATH= Util.flushDir(RESULT_FILE_PATH);
        accountID2TransferPreds = new ConcurrentHashMap<>();
        Environment environment = EnvironmentUtil.loadEnvironment(new String[0]);
        IPipelineResult result = AccountTransfer.submit(environment);
        PipelineResultCollect.get(result);
        environment.shutdown();
        /*for(Long AccountID: accountID2TransferPreds.keySet()){
            LOGGER.info("Account ID: "+AccountID+", Transfer predecessors: "+ accountID2TransferPreds.get(AccountID));
        }*/
    }

    public static IPipelineResult submit(Environment environment) {
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);
        //ResultValidator.cleanResult(RESULT_FILE_PATH);

        pipeline.submit((PipelineTask) pipelineTaskCxt -> {
            Configuration conf = pipelineTaskCxt.getConfig();
            PWindowSource<IVertex<Long,Double>> prVertices =
                    pipelineTaskCxt.buildSource(new AccountTransferSource<IVertex<Long,Double>>(DATA_PWD+"AccountTransferAccount.csv",
                                    (line, mp) -> {
                                        String[] fields = line.split("\\|");
                                        mp.putIfAbsent(Long.valueOf(fields[1]),new Vector<>());
                                        mp.get(Long.valueOf(fields[1])).add(Long.valueOf(fields[0]));
                                        return Collections.emptyList();
                                    }, AccountTransfer.accountID2TransferPreds), AllWindow.getInstance())
                            .withParallelism(conf.getInteger(Case1ConfigKeys.SOURCE_PARALLELISM));

            PWindowSource<IEdge<Long, Double>> prEdges = pipelineTaskCxt.buildSource(
                    new NullSource<IEdge<Long,Double>>()
                    , AllWindow.getInstance()
            ).withParallelism(conf.getInteger(Case1ConfigKeys.SOURCE_PARALLELISM));

            int iterationParallelism = conf.getInteger(Case1ConfigKeys.ITERATOR_PARALLELISM);
            GraphViewDesc graphViewDesc = GraphViewBuilder
                    .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                    .withShardNum(iterationParallelism)
                    .withBackend(IViewDesc.BackendType.Memory)
                    .build();
            PGraphWindow<Long, Double, Double> graphWindow =
                    pipelineTaskCxt.buildWindowStreamGraph(prVertices, prEdges, graphViewDesc);

            SinkFunction<IVertex<Long, Double>> sink = new NullSinkFunction();
            graphWindow.compute(new AccountTransfer.PRAlgorithms(10))
                    .compute(iterationParallelism)
                    .getVertices()
                    .sink(sink)
                    .withParallelism(conf.getInteger(Case1ConfigKeys.SINK_PARALLELISM));
        });

        return pipeline.execute();
    }

    public static class PRAlgorithms extends VertexCentricCompute<Long, Double, Double, Double> {

        public PRAlgorithms(long iterations) {
            super(iterations);
        }

        @Override
        public VertexCentricComputeFunction<Long, Double, Double, Double> getComputeFunction() {
            return new AccountTransfer.PRVertexCentricComputeFunction();
        }

        @Override
        public VertexCentricCombineFunction<Double> getCombineFunction() {
            return null;
        }

    }

    public static class PRVertexCentricComputeFunction extends AbstractVcFunc<Long, Double, Double, Double> {

        @Override
        public void compute(Long vertexId,
                            Iterator<Double> messageIterator) {
        }
    }
}
