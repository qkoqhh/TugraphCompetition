package com.antgroup.geaflow.casetwo;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

public class MySinkFunction extends RichFunction implements SinkFunction<IVertex<Long, VertexValue>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySinkFunction.class);
    public static final int CASEID=2;
    public String filePath;
    public File file;
    RuntimeContext runtimeContext;

    static Boolean firstOpen=false;
    static AtomicInteger runningThread=new AtomicInteger((Integer)MyConfigKeys.SINK_PARALLELISM.getDefaultValue());

    @Override
    public void open(RuntimeContext runtimeContext) {
        this.runtimeContext=runtimeContext;
        filePath = String.format("%sresult%s.csv", runtimeContext.getConfiguration().getString("output.dir"), CASEID);
        LOGGER.info("sink file name {}", filePath);

        synchronized (firstOpen) {
            if(!firstOpen) {
                firstOpen = true;

                file = new File(filePath);

                try {
                    if (file.exists()) {
                        FileUtils.forceDelete(file);
                    }

                    if (!file.exists()) {
                        if (!file.getParentFile().exists()) {
                            file.getParentFile().mkdirs();
                        }

                        file.createNewFile();
                    }

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }else{
                file = new File(filePath);
            }
        }
    }

    @Override
    public void close() {
        if(runningThread.addAndGet(-1)==0) {
            LOGGER.info("Close");
            list.sort(Comparator.comparing(IVertex::getId));
            StringBuilder stringBuilder = new StringBuilder();
            try {
                stringBuilder.append("id|value\n");
//            FileUtils.write(file,"id|value\n",Charset.defaultCharset());
                list.forEach(v -> {
                    stringBuilder.append(v.getId());
                    stringBuilder.append('|');
                    stringBuilder.append(v.getValue().ret);
                    stringBuilder.append('\n');
                });
                FileUtils.write(file, stringBuilder.toString(), Charset.defaultCharset(), true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static List<IVertex<Long, VertexValue>> list=new Vector<>();

    @Override
    public void write(IVertex<Long, VertexValue> out) throws Exception {
        if(out.getValue().ret>0) {
            list.add(out);
        }
    }
}

