package com.antgroup.geaflow.casethree;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import kotlin.Pair;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.util.Comparator;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MySinkFunction extends RichFunction implements SinkFunction<IVertex<Long,Double>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySinkFunction.class);
    public static final int CASEID=3;
    public String filePath;
    public File file;
    RuntimeContext runtimeContext;

    static Boolean firstOpen=false;
    static AtomicInteger runningThread=new AtomicInteger(0);

    @Override
    public void open(RuntimeContext runtimeContext) {
        this.runtimeContext=runtimeContext;
        filePath = String.format("%sresult%s.csv", runtimeContext.getConfiguration().getString("output.dir"), CASEID);
        LOGGER.info("sink file name {}", filePath);
        runningThread.addAndGet(1);

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

    static final NumberFormat format = NumberFormat.getInstance();
    static {
        format.setMaximumFractionDigits(2);
        format.setMinimumFractionDigits(2);
    }
    @Override
    public void close() {
        if(runningThread.addAndGet(-1)==0) {
            LOGGER.info("Close");
            list.sort(Comparator.comparing(IVertex::getId));
            StringBuilder stringBuilder = new StringBuilder();
            try {
                stringBuilder.append("id|value\n");
                list.forEach(v -> {
                    stringBuilder.append(v.getId());
                    stringBuilder.append('|');
                    stringBuilder.append(format.format(v.getValue()));
                    stringBuilder.append('\n');
                });
                FileUtils.write(file, stringBuilder.toString(), Charset.defaultCharset(), true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static List<IVertex<Long,Double>> list=new Vector();

    @Override
    public void write(IVertex<Long,Double> out) throws Exception {
        if(out.getValue()>0) {
            list.add(out);
        }
    }
}

