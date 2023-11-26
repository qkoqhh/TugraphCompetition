package com.antgroup.geaflow.fusion;

import com.antgroup.geaflow.Util.ThreadCounter;
import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.antgroup.geaflow.fusion.Fusion.EPS;

public class FusionSinkFunction extends RichFunction implements SinkFunction<IVertex<Pair<Long, VertexType>, VertexValue>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FusionSinkFunction.class);
    RuntimeContext runtimeContext;
    @Override
    public void open(RuntimeContext runtimeContext) {
        this.runtimeContext=runtimeContext;

    }


    static final NumberFormat format = NumberFormat.getInstance();
    static {
        format.setMaximumFractionDigits(2);
        format.setMinimumFractionDigits(2);
        format.setGroupingUsed(false);
    }
    static AtomicBoolean case1Output = new AtomicBoolean(false),
            case2Output = new AtomicBoolean(false),
            case3Output = new AtomicBoolean(false),
            case4Output = new AtomicBoolean(false);

    static ThreadCounter threadCounter=new ThreadCounter((Integer) MyConfigKeys.SINK_PARALLELISM.getDefaultValue());

    File openFile(int caseId){
        String filePath = String.format("%sresult%s.csv", runtimeContext.getConfiguration().getString("output.dir"), caseId);
        File file = new File(filePath);
        if (!file.exists()) {
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }

            try {
                file.createNewFile();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return file;
    }

    @Override
    public void close() {
        threadCounter.finish();
        threadCounter.check();
        if(case1Output.compareAndSet(false,true)) {
            LOGGER.info("Case 1 Sink");
            File file = openFile(1);
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("id|value\n");
            ans1.sort(Comparator.comparing(o->o.getId().getFirst()));
            ans1.forEach(v -> {
                stringBuilder.append(v.getId().getFirst())
                        .append('|')
                        .append(format.format(v.getValue().ret1 / 100000000D))
                        .append('\n');
            });
            try {
                FileUtils.write(file, stringBuilder.toString(), Charset.defaultCharset(), false);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            LOGGER.info("Finished Case 1 Sink");
        }
        if (case2Output.compareAndSet(false,true)){
            LOGGER.info("Case 2 Sink");
            File file=openFile(2);
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("id|value\n");
            ans2.sort(Comparator.comparing(o->o.getId().getFirst()));
            ans2.forEach(v->{
                stringBuilder.append(v.getId().getFirst())
                        .append('|')
                        .append(v.getValue().ret2)
                        .append('\n');
            });
            try {
                FileUtils.write(file, stringBuilder.toString(), Charset.defaultCharset(), false);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            LOGGER.info("Finished Case 2 Sink");
        }
        if (case3Output.compareAndSet(false,true)){
            LOGGER.info("Case 3 Sink");
            File file=openFile(3);
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("id|value\n");
            ans3.sort(Comparator.comparing(o->o.getId().getFirst()));
            ans3.forEach(v->{
                stringBuilder.append(v.getId().getFirst())
                        .append('|')
                        .append(format.format(v.getValue().ret3))
                        .append('\n');
            });
            try {
                FileUtils.write(file, stringBuilder.toString(), Charset.defaultCharset(), false);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            LOGGER.info("Finished Case 3 Sink");
        }
        if (case4Output.compareAndSet(false,true)){
            LOGGER.info("Case 4 Sink");
            File file=openFile(4);
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("id|value\n");
            ans4.sort(Comparator.comparing(o->o.getId().getFirst()));
            ans4.forEach(v->{
                stringBuilder.append(v.getId().getFirst())
                        .append('|')
                        .append(format.format(v.getValue().ret4/100000000D))
                        .append('\n');
            });
            try {
                FileUtils.write(file, stringBuilder.toString(), Charset.defaultCharset(), false);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            LOGGER.info("Finished Case 4 Sink");
        }
    }

    static List<IVertex<Pair<Long, VertexType>, VertexValue>> ans1 = new Vector<>() ,ans2 = new Vector<>(), ans3 = new Vector<>(), ans4 = new Vector<>();

    @Override
    public void write(IVertex<Pair<Long, VertexType>, VertexValue> out)  {
        VertexValue vv = out.getValue();
        if (vv.ret1 > EPS){
            ans1.add(out);
        }
        if (vv.ret2 > 0) {
            ans2.add(out);
        }
        if (vv.ret3 > EPS) {
            ans3.add(out);
        }
        if (vv.ret4 > EPS) {
            ans4.add(out);
        }
    }
}
