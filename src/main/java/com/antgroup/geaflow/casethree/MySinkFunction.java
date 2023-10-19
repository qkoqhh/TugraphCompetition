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
import java.util.Comparator;
import java.util.List;
import java.util.Vector;

public class MySinkFunction extends RichFunction implements SinkFunction<IVertex<Long,Double>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySinkFunction.class);
    public static final int CASEID=3;
    public String filePath;
    public File file;
    RuntimeContext runtimeContext;

    @Override
    public void open(RuntimeContext runtimeContext) {
        this.runtimeContext=runtimeContext;
        filePath = String.format("%s/result%s.csv", runtimeContext.getConfiguration().getString("output.dir"), CASEID);
        LOGGER.info("sink file name {}", filePath);
        file=new File(filePath);

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
    }

    @Override
    public void close() {
        LOGGER.info("Close");
        list.sort(Comparator.comparing(o->o.getId()));
        try {
            for (IVertex<Long, Double> v : list) {
                FileUtils.write(file, v.getId() + "|" + v.getValue() + "\n", Charset.defaultCharset(),true);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    List<IVertex<Long,Double>> list=new Vector();

    @Override
    public void write(IVertex<Long,Double> out) throws Exception {
        if(out.getValue()>0) {
            list.add(out);
        }
    }
}

