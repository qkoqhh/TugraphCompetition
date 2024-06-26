package com.antgroup.geaflow.fusion;


import org.apache.commons.math3.util.Pair;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class VertexValue {
    VertexValue(){}
    VertexValue(double amount){
        this.amount =  amount;
    }
    // Case 1
    public double ret1;


    // Case 2
    public int ret2;
    public Map<Long, Integer> inMap;
    public List<Long>  outArr;

    // Case 3
    public double ret3;

    // Case 4
    public double ret4;
    public Set<Long> guaranteeSet;
    public Double amount;
}

