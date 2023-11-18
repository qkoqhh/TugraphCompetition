package com.antgroup.geaflow.Util;

import com.antgroup.geaflow.fusion.VertexType;
import org.apache.commons.math3.util.Pair;

public class Util {
    public static String flushDir(String s){
        String ret=s.replace('\\','/');
        if(!ret.endsWith("/")){
            ret=ret+"/";
        }
        return ret;
    }

}
