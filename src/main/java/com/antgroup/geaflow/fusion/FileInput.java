package com.antgroup.geaflow.fusion;

import com.antgroup.geaflow.Util.ThreadCounter;
import com.antgroup.geaflow.Util.Util;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.edge.impl.ValueEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueVertex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.util.Pair;
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
    static final int SOURCE_PARALLELISM=(Integer) MyConfigKeys.SOURCE_PARALLELISM.getDefaultValue();

    static AtomicBoolean readLoan=new AtomicBoolean(false);
    static List<String> linesLoan = null;
    static ThreadCounter counterLoan = new ThreadCounter(1);
    static AtomicBoolean readLoanDepositAccount=new AtomicBoolean(false);
    static List<String> linesLoanDepositAccount = null;
    static ThreadCounter counterLoanDepositAccount= new ThreadCounter(1);
    static AtomicBoolean readPersonApplyLoan=new AtomicBoolean(false);
    static List<String> linesPersonApplyLoan = null;
    static ThreadCounter counterPersonApplyLoan= new ThreadCounter(1);
    static AtomicBoolean readPersonOwnAccount=new AtomicBoolean(false);
    static List<String> linesPersonOwnAccount = null;
    static ThreadCounter counterPersonOwnAccount= new ThreadCounter(1);
    static AtomicBoolean readAccountTransferAccount=new AtomicBoolean(false);
    static List<String> linesAccountTransferAccount = null;
    static ThreadCounter counterAccountTransferAccount= new ThreadCounter(1);
    static AtomicBoolean readPersonGuaranteePerson=new AtomicBoolean(false);
    static List<String> linesPersonGuaranteePerson = null;
    static ThreadCounter counterPersonGuaranteePerson= new ThreadCounter(1);
    static void readFile(String filePath){
        if(readLoan.compareAndSet(false,true)){
            linesLoan=readFileLines(filePath+"Loan.csv");
            counterLoan.finish();
        }
        if(readLoanDepositAccount.compareAndSet(false,true)) {
            linesLoanDepositAccount = readFileLines(filePath + "LoanDepositAccount.csv");
            counterLoanDepositAccount.finish();
        }
        if(readPersonApplyLoan.compareAndSet(false,true)){
            linesPersonApplyLoan= readFileLines(filePath+"PersonApplyLoan.csv");
            counterPersonApplyLoan.finish();
        }
        if(readPersonOwnAccount.compareAndSet(false,true)){
            linesPersonOwnAccount = readFileLines(filePath+"PersonOwnAccount.csv");
            counterPersonOwnAccount.finish();
        }
        if(readAccountTransferAccount.compareAndSet(false,true)){
            linesAccountTransferAccount = readFileLines(filePath+"AccountTransferAccount.csv");
            counterAccountTransferAccount.finish();
        }
        if (readPersonGuaranteePerson.compareAndSet(false,true)){
            linesPersonGuaranteePerson = readFileLines(filePath+"PersonGuaranteePerson.csv");
            counterPersonGuaranteePerson.finish();
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
    static ThreadCounter counterLoanAmount = new ThreadCounter(SOURCE_PARALLELISM);
    // TBD: use atomic var to handle lines
    static void readLoan(int parallel,int index){
        counterLoan.check();

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

        counterLoanAmount.finish();
    }

    static Map<Long,Double> account2loan = new ConcurrentHashMap<>();
    static ThreadCounter counterAccount2loan = new ThreadCounter(SOURCE_PARALLELISM);
    public static void readLoanDepositAccount(int parallel,int index){
        counterLoanDepositAccount.check();
        counterLoanAmount.check();

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

        counterAccount2loan.finish();
    }


    static Map<Long,Double> person2loan = new ConcurrentHashMap<>();
    static ThreadCounter counterPerson2loan = new ThreadCounter(SOURCE_PARALLELISM);
    public static void readPersonApplyLoan(int parallel,int index){
        counterPersonApplyLoan.check();
        counterLoanAmount.check();

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

        counterPerson2loan.finish();
    }

    static Map<Long,Long> accountOwner=new ConcurrentHashMap<>();
    static ThreadCounter counterAccountOwner = new ThreadCounter(SOURCE_PARALLELISM);
    public static void readPersonOwnAccount(int parallel, int index){
        counterPersonOwnAccount.check();

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

        counterAccountOwner.finish();
    }


    static ThreadCounter counterReadAccountTransferAccount = new ThreadCounter(SOURCE_PARALLELISM);
    static Set<Pair<Long, VertexType>> vertexSet=ConcurrentHashMap.newKeySet();
    public static void readAccountTransferAccount(int parallel, int index){
        LOGGER.info("parallel {}, index {}",parallel,index);
        List<IVertex<Pair<Long,VertexType>,VertexValue>> vertexList=vertexListArr.get(index);
        List<IEdge<Pair<Long,VertexType>,EdgeValue>> edgeList=edgeListArr.get(index);
        counterAccountTransferAccount.check();
        counterAccountOwner.check();

        int size = linesAccountTransferAccount.size();
        int readPos = Math.max (1, size *index /parallel);
        int readEnd = Math.min(size*(index+1)/parallel,size);
        for (int i=readPos; i<readEnd; i++) {
            String line = linesAccountTransferAccount.get(i);
            int first = line.indexOf('|');
            int second = line.indexOf('|', first + 1);
            int third = line.indexOf('|', second + 1);
            long fromId = Long.parseLong(line.substring(0, first));
            long toId = Long.parseLong(line.substring(first + 1, second));
            double amoumt = Double.parseDouble(line.substring(second+1, third));
            Pair<Long, VertexType> fromKey = new Pair<>(fromId, VertexType.Account);
            Pair<Long, VertexType> toKey = new Pair<>(toId, VertexType.Account);
            if (!vertexSet.contains(fromKey)) {
                vertexList.add(new ValueVertex<>(fromKey,
                        new VertexValue(accountOwner.getOrDefault(fromId,-1L))));
                vertexSet.add(fromKey);
                Fusion.transferIn.put(fromId, new HashMap<>());
                Fusion.transferOut.put(fromId, new HashMap<>());
            }
            if (!vertexSet.contains(toKey)) {
                vertexList.add(new ValueVertex<>(toKey,
                        new VertexValue(accountOwner.getOrDefault(toId,-1L))));
                vertexSet.add(toKey);
                Fusion.transferIn.put(toId, new HashMap<>());
                Fusion.transferOut.put(toId, new HashMap<>());
            }
            edgeList.add(new ValueEdge<>(fromKey, toKey, new EdgeValue(amoumt)));
            edgeList.add(new ValueEdge<>(toKey, fromKey, new EdgeValue(amoumt), EdgeDirection.IN));
        }

        counterReadAccountTransferAccount.finish();
    }



    static ThreadCounter counterReadPersonGuaranteePerson = new ThreadCounter(SOURCE_PARALLELISM);
    public static void readPersonGuaranteePerson(int parallel, int index){
        LOGGER.info("parallel {}, index {}",parallel,index);
        List<IVertex<Pair<Long, VertexType>,VertexValue>> vertexList=vertexListArr.get(index);
        List<IEdge<Pair<Long, VertexType>,EdgeValue>> edgeList=edgeListArr.get(index);
        counterPersonGuaranteePerson.check();
        counterAccountOwner.check();

        int size = linesPersonGuaranteePerson.size();
        int readPos = Math.max (1, size *index /parallel);
        int readEnd = Math.min(size*(index+1)/parallel,size);
        for (int i=readPos; i<readEnd; i++){
            String line= linesPersonGuaranteePerson.get(i);
            int first = line.indexOf('|');
            int second = line.indexOf('|',first+1);
            long fromId = Long.parseLong(line.substring(0, first));
            long toId = Long.parseLong(line.substring(first + 1, second));
            Pair<Long, VertexType> fromKey = new Pair<>(fromId, VertexType.Person);
            Pair<Long, VertexType> toKey = new Pair<>(toId, VertexType.Person);
            if (!vertexSet.contains(fromKey)) {
                vertexList.add(new ValueVertex<>(fromKey,
                        new VertexValue(accountOwner.getOrDefault(fromId,-1L))));
                vertexSet.add(fromKey);
            }
            if (!vertexSet.contains(toKey)) {
                vertexList.add(new ValueVertex<>(toKey,
                        new VertexValue(accountOwner.getOrDefault(toId,-1L))));
                vertexSet.add(toKey);
            }
            edgeList.add(new ValueEdge<>(toKey, fromKey, new EdgeValue(0), EdgeDirection.IN));
        }

        counterReadPersonGuaranteePerson.finish();
    }



    static final List<List<IVertex<Pair<Long,VertexType>,VertexValue>>> vertexListArr=new ArrayList<List<IVertex<Pair<Long, VertexType>, VertexValue>>>(SOURCE_PARALLELISM)
    {{
        for (int i=0;i<SOURCE_PARALLELISM;i++) add(new ArrayList<>());
    }};
    static final List<List<IEdge<Pair<Long,VertexType>,EdgeValue>>> edgeListArr=new ArrayList<List<IEdge<Pair<Long, VertexType>, EdgeValue>>>(SOURCE_PARALLELISM)
    {{
        for (int i=0;i<SOURCE_PARALLELISM;i++) add(new ArrayList<>());
    }};
}
