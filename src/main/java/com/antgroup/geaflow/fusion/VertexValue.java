package com.antgroup.geaflow.fusion;


import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class VertexValue {
    VertexValue(long owner){
        this.owner=owner;
        this.guaranteeSet = new HashSet<>();
    }
    // Case 1
    public long owner;


    // Case 2
    public int ret2;

    // Case 3
    public double ret3;

    // Case 4
    public double ret4;
    public Set<Long> guaranteeSet;
}

