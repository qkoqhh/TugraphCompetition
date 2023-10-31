package com.antgroup.geaflow.case1and4.case4;

import com.antgroup.geaflow.case1and4.case4.GuaranteeGraph.GuaranteeGraph;
import com.antgroup.geaflow.case1and4.case4.LoanAmount.LoanAmount;
import com.antgroup.geaflow.case1and4.case4.PersonLoan.PersonLoan;

public class Run {
    public static void main(String[] args) {
        LoanAmount.main(args);
        PersonLoan.main(args);
        GuaranteeGraph.main(args);
    }
}
