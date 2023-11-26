package com.antgroup.geaflow.fusion;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.io.SourceFunction;
import com.antgroup.geaflow.api.window.IWindow;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueEdge;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class FusionEdgeSource extends RichFunction implements SourceFunction<IEdge<Pair<Long, VertexType>,Double>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FusionEdgeSource.class);
    protected final String filePath;
    protected transient RuntimeContext runtimeContext;

    public FusionEdgeSource(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void open(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    static List<String> readFileLines(String filePath) {
        try {
            List<String> lines = FileUtils.readLines(new File(filePath), Charset.defaultCharset());
            return lines;
        } catch (IOException e) {
            throw new RuntimeException("error in read resource file: " + filePath, e);
        }
    }

    List<String> linesLoanDepositAccount,linesPersonApplyLoan,linesAccountTransferAccount,linesPersonGuaranteePerson,linesPersonOwnAccount;
    void readFile(){
        linesLoanDepositAccount = readFileLines(filePath + "LoanDepositAccount.csv");
        linesPersonApplyLoan= readFileLines(filePath+"PersonApplyLoan.csv");
        linesPersonOwnAccount = readFileLines(filePath+"PersonOwnAccount.csv");
        linesAccountTransferAccount = readFileLines(filePath+"AccountTransferAccount.csv");
        linesPersonGuaranteePerson = readFileLines(filePath+"PersonGuaranteePerson.csv");
    }

    public void readPersonOwnAccount(int parallel,int index){
        int size = linesPersonOwnAccount.size();
        int readPos = Math.max (1, size *index /parallel);
        int readEnd = Math.min(size*(index+1)/parallel,size);
        for (int i=readPos; i<readEnd; i++){
            String line= linesPersonOwnAccount.get(i);
            int first = line.indexOf('|');
            int second = line.indexOf('|',first+1);
            long personId = Long.parseLong(line.substring(0,first));
            long accountId = Long.parseLong(line.substring(first+1,second));
            record.add(new ValueEdge<>(new Pair<>(accountId,VertexType.Account),new Pair<>(personId, VertexType.Person), 0D, EdgeDirection.IN));
        }
    }
    public void readLoanDepositAccount(int parallel,int index){
        int size = linesLoanDepositAccount.size();
        int readPos = Math.max (1, size *index /parallel);
        int readEnd = Math.min(size*(index+1)/parallel,size);
        for (int i=readPos; i<readEnd; i++){
            String line= linesLoanDepositAccount.get(i);
            int first = line.indexOf('|');
            int second = line.indexOf('|',first+1);
            long loanId = Long.parseLong(line.substring(0,first));
            long accountId = Long.parseLong(line.substring(first+1,second));
            record.add(new ValueEdge<>(new Pair<>(loanId, VertexType.Loan),new Pair<>(accountId,VertexType.Account), 0D));;
        }
    }
    public void readPersonApplyLoan(int parallel,int index){
        int size = linesPersonApplyLoan.size();
        int readPos = Math.max (1, size *index /parallel);
        int readEnd = Math.min(size*(index+1)/parallel,size);
        for (int i=readPos; i<readEnd; i++){
            String line= linesPersonApplyLoan.get(i);
            int first = line.indexOf('|');
            int second = line.indexOf('|',first+1);
            long personId = Long.parseLong(line.substring(0,first));
            long loanId = Long.parseLong(line.substring(first+1,second));
            record.add(new ValueEdge<>(new Pair<>(loanId, VertexType.Loan),new Pair<>(personId,VertexType.Person), 0D, EdgeDirection.IN));;
        }
    }
    public void readAccountTransferAccount(int parallel,int index){
        int size = linesAccountTransferAccount.size();
        int readPos = Math.max (1, size *index /parallel);
        int readEnd = Math.min(size*(index+1)/parallel,size);
        for (int i=readPos; i<readEnd; i++){
            String line= linesAccountTransferAccount.get(i);
            int first = line.indexOf('|');
            int second = line.indexOf('|', first + 1);
            int third = line.indexOf('|', second + 1);
            long fromId = Long.parseLong(line.substring(0, first));
            long toId = Long.parseLong(line.substring(first + 1, second));
            double amount = Double.parseDouble(line.substring(second+1, third));
            Pair<Long, VertexType> fromKey = new Pair<>(fromId, VertexType.Account);
            Pair<Long, VertexType> toKey = new Pair<>(toId, VertexType.Account);
            record.add(new ValueEdge<>(fromKey, toKey, amount));
            record.add(new ValueEdge<>(toKey, fromKey, amount, EdgeDirection.IN));
        }
    }
    public void readPersonGuaranteePerson(int parallel,int index){
        int size = linesPersonGuaranteePerson.size();
        int readPos = Math.max (1, size *index /parallel);
        int readEnd = Math.min(size*(index+1)/parallel,size);
        for (int i=readPos; i<readEnd; i++){
            String line= linesPersonGuaranteePerson.get(i);
            int first = line.indexOf('|');
            int second = line.indexOf('|', first + 1);
            long fromId = Long.parseLong(line.substring(0, first));
            long toId = Long.parseLong(line.substring(first + 1, second));
            Pair<Long, VertexType> fromKey = new Pair<>(fromId, VertexType.Person);
            Pair<Long, VertexType> toKey = new Pair<>(toId, VertexType.Person);

            record.add(new ValueEdge<>(toKey, fromKey, 0D,EdgeDirection.IN));
        }
    }


    List<IEdge<Pair<Long,VertexType>,Double>>record=new ArrayList<>();
    int readPos, listSize;
    @Override
    public void init(int parallel, int index) {
        readFile();
        readPersonOwnAccount(parallel, index);
        readLoanDepositAccount(parallel, index);
        readPersonApplyLoan(parallel, index);
        readAccountTransferAccount(parallel, index);
        readPersonGuaranteePerson(parallel, index);

        readPos = 0;
        listSize = record.size();
    }

    @Override
    public boolean fetch(IWindow<IEdge<Pair<Long, VertexType>, Double>> window, SourceContext<IEdge<Pair<Long, VertexType>, Double>> ctx) throws Exception {
        LOGGER.info("collection source fetch taskId:{}, batchId:{}",
                runtimeContext.getTaskArgs().getTaskId(), window.windowId());
        while (readPos < listSize) {
            IEdge<Pair<Long, VertexType>, Double> out = record.get(readPos);
            long windowsId = window.assignWindow(out);
            if (window.windowId() == windowsId) {
                ctx.collect(out);
                readPos++;
            } else {
                break;
            }
        }
        return readPos < listSize;
    }

    @Override
    public void close() {

    }
}
