package com.antgroup.geaflow.case1and4.case1;

import com.antgroup.geaflow.case1and4.case1.AccountLoan.AccountLoan;
import com.antgroup.geaflow.case1and4.case1.AccountTransfer.AccountTransfer;
import com.antgroup.geaflow.case1and4.case1.PersonValue.PersonValue;
import com.antgroup.geaflow.case1and4.case4.LoanAmount.LoanAmount;

public class Run {
    public static void main(String[] args){
        LoanAmount.main(args);
        AccountLoan.main(args);
        AccountTransfer.main(args);
        PersonValue.main(args);
    }
}
