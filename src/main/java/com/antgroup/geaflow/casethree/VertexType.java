package com.antgroup.geaflow.casethree;

public enum VertexType {
    Acccount,
    Loan,
    Person;

    public static VertexType getVertexType(String str) {
        if("Account".equals(str))return Acccount;
        if("Loan".equals(str))return Loan;
        if("Person".equals(str))return Person;
        return null;
    }
}


