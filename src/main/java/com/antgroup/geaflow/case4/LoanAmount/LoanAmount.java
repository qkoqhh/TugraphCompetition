package com.antgroup.geaflow.case4.LoanAmount;

import com.antgroup.geaflow.api.function.base.FilterFunction;
import com.antgroup.geaflow.api.function.base.FlatMapFunction;
import com.antgroup.geaflow.api.function.base.KeySelector;
import com.antgroup.geaflow.api.function.base.MapFunction;
import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.function.io.SourceFunction;
import com.antgroup.geaflow.api.graph.PGraphWindow;
import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import com.antgroup.geaflow.api.pdata.PStreamSink;
import com.antgroup.geaflow.api.pdata.PStreamSource;
import com.antgroup.geaflow.api.pdata.PWindowCollect;
import com.antgroup.geaflow.api.pdata.stream.PStream;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowBroadcastStream;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowKeyStream;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowStream;
import com.antgroup.geaflow.api.window.IWindow;
import com.antgroup.geaflow.api.window.impl.AllWindow;
import com.antgroup.geaflow.casethree.MyConfigKeys;
import com.antgroup.geaflow.casethree.MyFileSource;
import com.antgroup.geaflow.casethree.MySinkFunction;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.example.config.ExampleConfigKeys;
import com.antgroup.geaflow.example.function.AbstractVcFunc;
import com.antgroup.geaflow.example.function.FileSink;
import com.antgroup.geaflow.example.function.FileSource;
import com.antgroup.geaflow.example.util.EnvironmentUtil;
import com.antgroup.geaflow.example.util.ExampleSinkFunctionFactory;
import com.antgroup.geaflow.example.util.PipelineResultCollect;
import com.antgroup.geaflow.example.util.ResultValidator;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueVertex;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.pipeline.Pipeline;
import com.antgroup.geaflow.pipeline.PipelineFactory;
import com.antgroup.geaflow.pipeline.context.IPipelineContext;
import com.antgroup.geaflow.pipeline.task.PipelineTask;
import com.antgroup.geaflow.view.GraphViewBuilder;
import com.antgroup.geaflow.view.IViewDesc;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class LoanAmount {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoanAmountSource.class);

    public static String DATA_PWD = "./src/main/resources/snapshot/";
    public static final String RESULT_FILE_PATH = "./target/tmp/data/result/LoanAmount";
//    public static final String RESULT_FILE_PATH = "C:\\Users\\qkoqhh\\IdeaProjects\\TuGraphPageRank\\target\\pagerank";

    public static Map<Long,Double> loanID2Amount = null;


    public static void main(String[] args) {
        loanID2Amount = new HashMap<>();
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        IPipelineResult result = LoanAmount.submit(environment);
        PipelineResultCollect.get(result);
        environment.shutdown();
        for(Long loanID:loanID2Amount.keySet()){
            LOGGER.info("ID: "+loanID+", Amount: "+loanID2Amount.get(loanID));
        }
    }

    public static IPipelineResult submit(Environment environment) {
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);
        ResultValidator.cleanResult(RESULT_FILE_PATH);

        pipeline.submit((PipelineTask) pipelineTaskCxt -> {
            Configuration conf = pipelineTaskCxt.getConfig();
            PWindowSource<IVertex<Long,Double>> prVertices =
                    pipelineTaskCxt.buildSource(new LoanAmountSource<IVertex<Long,Double>>(DATA_PWD+"Loan.csv",
                            (line, mp) -> {
                                        String[] fields = line.split("\\|");
                                        IVertex<Long, Double> vertex = new ValueVertex<>(
                                                Long.valueOf(fields[0]), Double.valueOf(fields[1]));
                                        mp.put(Long.valueOf(fields[0]), Double.valueOf(fields[1]));
                                        return Collections.emptyList();
                                    }, loanID2Amount), AllWindow.getInstance())
                            .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));

            PWindowSource<IEdge<Long, Double>> prEdges = pipelineTaskCxt.buildSource(
                    new LoanAmountSource<IEdge<Long,Double>>(DATA_PWD + "Loan.csv",
                            (line, mp) -> {
                                return Collections.emptyList();
                            },
                            null
                    ), AllWindow.getInstance()
            ).withParallelism(conf.getInteger(MyConfigKeys.SOURCE_PARALLELISM));

            int iterationParallelism = conf.getInteger(ExampleConfigKeys.ITERATOR_PARALLELISM);
            GraphViewDesc graphViewDesc = GraphViewBuilder
                    .createGraphView(GraphViewBuilder.DEFAULT_GRAPH)
                    .withShardNum(2)
                    .withBackend(IViewDesc.BackendType.Memory)
                    .build();
            PGraphWindow<Long, Double, Double> graphWindow =
                    pipelineTaskCxt.buildWindowStreamGraph(prVertices, prEdges, graphViewDesc);

            SinkFunction<IVertex<Long, Double>> sink = new MySinkFunction();
            graphWindow.compute(new LoanAmount.PRAlgorithms(10))
                    .compute(iterationParallelism)
                    .getVertices()
                    .sink(sink)
//                    .sink(v -> {
//                        LOGGER.info("result {}", v);
//                    })
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SINK_PARALLELISM));
        });

        return pipeline.execute();
    }

    public static class PRAlgorithms extends VertexCentricCompute<Long, Double, Double, Double> {

        public PRAlgorithms(long iterations) {
            super(iterations);
        }

        @Override
        public VertexCentricComputeFunction<Long, Double, Double, Double> getComputeFunction() {
            return new LoanAmount.PRVertexCentricComputeFunction();
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
