package com.antgroup.geaflow.case1and4.case4.GuaranteeGraph;

import java.util.HashMap;
import java.util.Map;

public class VertexInfo {
    public Double value;
    public Map<Long,Integer> minDistMap;
    public VertexInfo(){
        value=0.0;
        minDistMap=new HashMap<>();
    }
}
