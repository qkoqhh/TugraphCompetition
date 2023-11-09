package com.antgroup.geaflow.case1and4.case1.PersonValue;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.case1and4.case4.GuaranteeGraph.GuaranteeGraphSinkFunction;
import com.antgroup.geaflow.case1and4.case4.GuaranteeGraph.VertexInfo;
import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.IDVertex;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.List;
import java.util.Vector;

public class PersonValueSinkFunction extends RichFunction implements SinkFunction<IVertex<Tuple<Long,Boolean>,Double>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PersonValueSinkFunction.class);
    public static final int CASEID=1;
    public String filePath;
    public File file;
    RuntimeContext runtimeContext;

    static Boolean firstOpen=false;
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
        LOGGER.info("Close");
        list.sort(Comparator.comparing(Tuple::getF0));
        StringBuilder stringBuilder=new StringBuilder();
        try {
            stringBuilder.append("id|value\n");
//            FileUtils.write(file,"id|value\n",Charset.defaultCharset());
            list.forEach(v->{
                stringBuilder.append(v.getF0());
                stringBuilder.append("|");
                stringBuilder.append(String.format("%.2f",v.getF1()));
                stringBuilder.append("\n");
            });
            FileUtils.write(file,stringBuilder.toString(), Charset.defaultCharset(),true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    List<Tuple<Long, Double>> list=new Vector<>();
    @Override
    public void write(IVertex<Tuple<Long,Boolean>, Double> out) throws Exception {
        if(out.getId().getF1() && out.getValue()>0) {
            list.add(new Tuple<>(out.getId().getF0(),out.getValue()));
        }
    }
}
