package com.antgroup.geaflow.fusion;

import com.antgroup.geaflow.Util.Util;
import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.io.SourceFunction;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueVertex;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class FileInput{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileInput.class);

    static AtomicBoolean readLoan=new AtomicBoolean(false), finishLoan = new AtomicBoolean(false);
    static List<String> linesLoan = null;
    static AtomicBoolean readLoanDepositAccount=new AtomicBoolean(false), finishLoanDepositAccount = new AtomicBoolean(false);
    static List<String> linesLoanDepositAccount = null;
    static AtomicBoolean readPersonApplyLoan=new AtomicBoolean(false), finishPersonApplyLoan = new AtomicBoolean(false);
    static List<String> linesPersonApplyLoan = null;
    static AtomicBoolean readPersonOwnAccount=new AtomicBoolean(false), finishPersonOwnAccount= new AtomicBoolean(false);
    static List<String> linesPersonOwnAccount = null;
    static AtomicBoolean readAccountTransferAccount=new AtomicBoolean(false), finishAccountTransferAccount= new AtomicBoolean(false);
    static List<String> linesAccountTransferAccount = null;
    static AtomicBoolean readPersonGuaranteePerson=new AtomicBoolean(false), finishPersonGuaranteePerson= new AtomicBoolean(false);
    static List<String> linesPersonGuaranteePerson = null;
    static void readFile(String filePath){
        if(readLoan.compareAndSet(false,true)){
            linesLoan=readFileLines(filePath+"Loan.csv");
            finishLoan.set(true);
        }
        if(readLoanDepositAccount.compareAndSet(false,true)) {
            linesLoanDepositAccount = readFileLines(filePath + "LoanDepositAccount.csv");
            finishLoanDepositAccount.set(true);
        }
        if(readPersonApplyLoan.compareAndSet(false,true)){
            linesPersonApplyLoan= readFileLines(filePath+"PersonApplyLoan.csv");
            finishPersonApplyLoan.set(true);
        }
        if(readPersonOwnAccount.compareAndSet(false,true)){
            linesPersonOwnAccount = readFileLines(filePath+"PersonOwnAccount.csv");
            finishPersonOwnAccount.set(true);
        }
        if(readAccountTransferAccount.compareAndSet(false,true)){
            linesAccountTransferAccount = readFileLines(filePath+"AccountTransferAccount.csv");
            finishAccountTransferAccount.set(true);
        }
        if (readPersonGuaranteePerson.compareAndSet(false,true)){
            linesPersonGuaranteePerson = readFileLines(filePath+"PersonGuaranteePerson.csv");
            finishPersonGuaranteePerson.set(true);
        }
    }

    static List<String> readFileLines(String filePath) {
        try {
            List<String> lines = FileUtils.readLines(new File(filePath), Charset.defaultCharset());
            return lines;
        } catch (IOException e) {
            throw new RuntimeException("error in read resource file: " + filePath, e);
        }
    }




    static Map<Long,Double> loanAmount = new ConcurrentHashMap<>();
    // TBD: use atomic var to handle lines
    static void readLoan(int parallel,int index){
        while(!finishLoan.get());
        int size = linesLoan.size();
        int readPos = Math.max (1, size *index /parallel);
        int readEnd = Math.min(size*(index+1)/parallel,size);
        for (int i=readPos; i<readEnd; i++){
            String line= linesLoan.get(i);
            int first = line.indexOf('|');
            int second = line.indexOf('|',first+1);
            long loanId = Long.parseLong(line.substring(0,first));
            double amount = Double.parseDouble(line.substring(first+1,second));
            loanAmount.put(loanId, amount);
        }
    }

    static Map<Long,Double> account2loan = new ConcurrentHashMap<>();
    public static void readLoanDepositAccount(int parallel,int index){
        while(!finishLoanDepositAccount.get());
        while(!finishLoan.get());
        int size = linesLoanDepositAccount.size();
        int readPos = Math.max (1, size *index /parallel);
        int readEnd = Math.min(size*(index+1)/parallel,size);
        for (int i=readPos; i<readEnd; i++){
            String line= linesLoanDepositAccount.get(i);
            int first = line.indexOf('|');
            int second = line.indexOf('|',first+1);
            long loanId = Long.parseLong(line.substring(0,first));
            long accountId = Long.parseLong(line.substring(first+1,second));
            double amount = loanAmount.get(loanId);
            account2loan.compute(accountId, (k,v)-> (v==null)?amount:v+amount);
        }
    }


    static Map<Long,Double> person2loan = new ConcurrentHashMap<>();
    public static void readPersonApplyLoan(int parallel,int index){
        while(!finishPersonApplyLoan.get());
        while(!finishLoan.get());
        int size = linesPersonApplyLoan.size();
        int readPos = Math.max (1, size *index /parallel);
        int readEnd = Math.min(size*(index+1)/parallel,size);
        for (int i=readPos; i<readEnd; i++){
            String line= linesPersonApplyLoan.get(i);
            int first = line.indexOf('|');
            int second = line.indexOf('|',first+1);
            long personId = Long.parseLong(line.substring(0,first));
            long loanId = Long.parseLong(line.substring(first+1,second));
            double amount = loanAmount.get(loanId);
            person2loan.compute(personId, (k,v)-> (v==null)?amount:v+amount);
        }
    }

    static Map<Long,Long> accountOwner=new ConcurrentHashMap<>();
    public static void readPersonOwnAccount(int parallel, int index){
        while(!finishPersonOwnAccount.get());
        int size = linesPersonOwnAccount.size();
        int readPos = Math.max (1, size *index /parallel);
        int readEnd = Math.min(size*(index+1)/parallel,size);
        for (int i=readPos; i<readEnd; i++){
            String line= linesPersonOwnAccount.get(i);
            int first = line.indexOf('|');
            int second = line.indexOf('|',first+1);
            long personId = Long.parseLong(line.substring(0,first));
            long accountId = Long.parseLong(line.substring(first+1,second));
            accountOwner.put(accountId, personId);
        }
    }


    static Set<Long> vertexSet=ConcurrentHashMap.newKeySet();
    public static void readAccountTransferAccount(int parallel, int index){
        List<IVertex<Long,VertexValue>> vertexList=vertexListArr.get(index);
        List<IEdge<Long,EdgeValue>> edgeList=edgeListArr.get(index);
        while(!finishAccountTransferAccount.get());
        int size = linesAccountTransferAccount.size();
        int readPos = Math.max (1, size *index /parallel);
        int readEnd = Math.min(size*(index+1)/parallel,size);
        for (int i=readPos; i<readEnd; i++) {
            String line = linesAccountTransferAccount.get(i);
            int first = line.indexOf('|');
            int second = line.indexOf('|', first + 1);
            long fromId = Util.flushId(Long.parseLong(line.substring(0, first)), VertexType.Account);
            long toId = Util.flushId(Long.parseLong(line.substring(first + 1, second)), VertexType.Account);
            if (!vertexSet.contains(fromId)) {
                vertexList.add(new ValueVertex<>(fromId, new VertexValue()));
                vertexSet.add(fromId);
            }
            if (!vertexSet.contains(toId)) {
                vertexList.add(new ValueVertex<>(toId, new VertexValue()));
                vertexSet.add(toId);
            }
            edgeList.add(new ValueEdge<>(fromId, toId, new EdgeValue()));
            edgeList.add(new ValueEdge<>(toId, fromId, new EdgeValue(), EdgeDirection.IN));
        }
    }



    public static void readPersonGuaranteePerson(int parallel, int index){
        List<IVertex<Long,VertexValue>> vertexList=vertexListArr.get(index);
        List<IEdge<Long,EdgeValue>> edgeList=edgeListArr.get(index);
        while(!finishPersonGuaranteePerson.get());
        int size = linesPersonGuaranteePerson.size();
        int readPos = Math.max (1, size *index /parallel);
        int readEnd = Math.min(size*(index+1)/parallel,size);
        for (int i=readPos; i<readEnd; i++){
            String line= linesPersonGuaranteePerson.get(i);
            int first = line.indexOf('|');
            int second = line.indexOf('|',first+1);
            long fromId = Util.flushId(Long.parseLong(line.substring(0, first)), VertexType.Person);
            long toId = Util.flushId(Long.parseLong(line.substring(first + 1, second)), VertexType.Person);
            if (!vertexSet.contains(fromId)) {
                vertexList.add(new ValueVertex<>(fromId, new VertexValue()));
                vertexSet.add(fromId);
            }
            if (!vertexSet.contains(toId)) {
                vertexList.add(new ValueVertex<>(toId, new VertexValue()));
                vertexSet.add(toId);
            }
            edgeList.add(new ValueEdge<>(fromId, toId, new EdgeValue()));
            edgeList.add(new ValueEdge<>(toId, fromId, new EdgeValue(), EdgeDirection.IN));
        }
    }



    static final int SOURCE_PARALLELISM=(Integer) MyConfigKeys.SOURCE_PARALLELISM.getDefaultValue();
    static final List<List<IVertex<Long,VertexValue>>> vertexListArr=new ArrayList<>(
            Collections.nCopies(SOURCE_PARALLELISM, new ArrayList<>()));
    static final List<List<IEdge<Long,EdgeValue>>> edgeListArr=new ArrayList<>(
            Collections.nCopies(SOURCE_PARALLELISM, new ArrayList<>()));
}
