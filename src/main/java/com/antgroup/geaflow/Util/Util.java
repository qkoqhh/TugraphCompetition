package com.antgroup.geaflow.Util;

import com.antgroup.geaflow.fusion.VertexType;

public class Util {
    public static String flushDir(String s){
        String ret=s.replace('\\','/');
        if(!ret.endsWith("/")){
            ret=ret+"/";
        }
        return ret;
    }

    public static long flushId(long id, VertexType type){
        return type==VertexType.Account ? (id<<1) : (id<<1|1);
    }
}
