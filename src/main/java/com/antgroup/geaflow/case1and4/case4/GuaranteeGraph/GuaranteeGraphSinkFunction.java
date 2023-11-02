package com.antgroup.geaflow.case1and4.case4.GuaranteeGraph;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.casetwo.VertexValue;
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

public class GuaranteeGraphSinkFunction extends RichFunction implements SinkFunction<IVertex<Long,VertexInfo>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GuaranteeGraphSinkFunction.class);
    public static final int CASEID=4;
    public String filePath;
    public File file;
    RuntimeContext runtimeContext;

    static Boolean firstOpen=false;

    @Override
    public void open(RuntimeContext runtimeContext) {
        this.runtimeContext=runtimeContext;
        filePath = String.format("%s/result%s.csv", runtimeContext.getConfiguration().getString("output.dir"), CASEID);
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
        LOGGER.info("Close");
        list.sort(Comparator.comparing(IVertex::getId));
        StringBuilder stringBuilder=new StringBuilder();
        try {
            stringBuilder.append("id|value\n");
//            FileUtils.write(file,"id|value\n",Charset.defaultCharset());
            list.forEach(v->{
                stringBuilder.append(v.getId());
                stringBuilder.append('|');
                stringBuilder.append(String.format("%.2f",v.getValue().value));
                stringBuilder.append('\n');
            });
            FileUtils.write(file,stringBuilder.toString(), Charset.defaultCharset(),true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    List<IVertex<Long, VertexInfo>> list=new Vector<>();

    @Override
    public void write(IVertex<Long, VertexInfo> out) throws Exception {
        if(out.getValue().value>0) {
            list.add(out);
        }
    }
}
