package com.antgroup.geaflow.Util;

public class Util {
    public static String flushDir(String s){
        String ret=s.replace('\\','/');
        if(!ret.endsWith("/")){
            ret=ret+"/";
        }
        return ret;
    }
}
