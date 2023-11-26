package com.antgroup.geaflow.fusion;

import com.antgroup.geaflow.Util.PartionFileInput;
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


    PartionFileInput inputLoanDepositAccount,inputPersonApplyLoan,inputAccountTransferAccount,inputPersonGuaranteePerson,inputPersonOwnAccount;

    public void readPersonOwnAccount(int parallel,int index) throws IOException {
        inputPersonOwnAccount = new PartionFileInput(filePath + "PersonOwnAccount.csv", parallel,index);
        while(inputPersonOwnAccount.nextLine()){
            long personId = inputPersonOwnAccount.nextLong();
            long accountId = inputPersonOwnAccount.nextLong();
            record.add(new ValueEdge<>(new Pair<>(accountId,VertexType.Account),new Pair<>(personId, VertexType.Person), 0D, EdgeDirection.IN));
        }
    }
    public void readLoanDepositAccount(int parallel,int index) throws IOException {
        inputLoanDepositAccount = new PartionFileInput(filePath+"LoanDepositAccount.csv", parallel,index);
        while(inputLoanDepositAccount.nextLine()){
            long loanId = inputLoanDepositAccount.nextLong();
            long accountId = inputLoanDepositAccount.nextLong();
            record.add(new ValueEdge<>(new Pair<>(loanId, VertexType.Loan),new Pair<>(accountId,VertexType.Account), 0D));;
        }
    }
    public void readPersonApplyLoan(int parallel,int index) throws IOException {
        inputPersonApplyLoan = new PartionFileInput(filePath+"PersonApplyLoan.csv",parallel,index);
        while(inputPersonApplyLoan.nextLine()){
            long personId = inputPersonApplyLoan.nextLong();
            long loanId = inputPersonApplyLoan.nextLong();
            record.add(new ValueEdge<>(new Pair<>(loanId, VertexType.Loan),new Pair<>(personId,VertexType.Person), 0D, EdgeDirection.IN));;
        }
    }
    public void readAccountTransferAccount(int parallel,int index) throws IOException {
        inputAccountTransferAccount = new PartionFileInput(filePath+"AccountTransferAccount.csv",parallel,index);
        while(inputAccountTransferAccount.nextLine()){
            long fromId = inputAccountTransferAccount.nextLong();
            long toId = inputAccountTransferAccount.nextLong();
            double amount = inputAccountTransferAccount.nextDouble();
            Pair<Long, VertexType> fromKey = new Pair<>(fromId, VertexType.Account);
            Pair<Long, VertexType> toKey = new Pair<>(toId, VertexType.Account);
            record.add(new ValueEdge<>(fromKey, toKey, amount));
            record.add(new ValueEdge<>(toKey, fromKey, amount, EdgeDirection.IN));
        }
    }
    public void readPersonGuaranteePerson(int parallel,int index) throws IOException {
        inputPersonGuaranteePerson = new PartionFileInput(filePath+"PersonGuaranteePerson.csv",parallel,index);
        while(inputPersonGuaranteePerson.nextLine()){
            long fromId = inputPersonGuaranteePerson.nextLong();
            long toId = inputPersonGuaranteePerson.nextLong();
            Pair<Long, VertexType> fromKey = new Pair<>(fromId, VertexType.Person);
            Pair<Long, VertexType> toKey = new Pair<>(toId, VertexType.Person);

            record.add(new ValueEdge<>(toKey, fromKey, 0D,EdgeDirection.IN));
        }
    }


    List<IEdge<Pair<Long,VertexType>,Double>>record=new ArrayList<>();
    int readPos, listSize;
    @Override
    public void init(int parallel, int index) {
        try {
            readPersonOwnAccount(parallel, index);
            readLoanDepositAccount(parallel, index);
            readPersonApplyLoan(parallel, index);
            readAccountTransferAccount(parallel, index);
            readPersonGuaranteePerson(parallel, index);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

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
