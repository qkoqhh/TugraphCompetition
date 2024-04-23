package com.antgroup.geaflow.case1and4.Nulls;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.io.SourceFunction;
import com.antgroup.geaflow.api.window.IWindow;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

public class NullSource<OUT> extends RichFunction implements SourceFunction<OUT> {
    private static final Logger LOGGER = LoggerFactory.getLogger(NullSource.class);

    static protected Map<String, List<String>> lineMap = new HashMap<>();
    protected transient RuntimeContext runtimeContext;


    public NullSource() {

    }

    @Override
    public void open(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    protected int readPos,readEnd;
    protected List<String> lines;
    protected List<OUT> record;
    @Override
    public void init(int parallel, int index) {
        LOGGER.info("NullSource fAKe: Parallel {} index {}",parallel,index);
    }

    @Override
    public boolean fetch(IWindow<OUT> window, SourceContext<OUT> ctx) throws Exception {
        LOGGER.info("collection source fetch taskId:{}, batchId:{}, start readPos {}, totalSize {}",
                runtimeContext.getTaskArgs().getTaskId(), window.windowId(), readPos, lineMap.size());
        while (readPos < readEnd) {
            OUT out = record.get(readPos);
            long windowId = window.assignWindow(out);
            if (window.windowId() == windowId) {
                ctx.collect(out);
                readPos++;
            } else {
                break;
            }
        }
        boolean result = false;
        if (readPos <  readEnd ){
            result = true;
        }
        LOGGER.info("collection source fetch batchId:{}, current readPos {}, result {}",
                window.windowId(), readPos, result);
        return result;
    }

    @Override
    public void close() {
    }

    private List<String> readFileLines(String filePath) {
        try {
            List<String> lines = FileUtils.readLines(new File(filePath), Charset.defaultCharset());
            return lines;
        } catch (IOException e) {
            throw new RuntimeException("error in read resource file: " + filePath, e);
        }
    }

}
