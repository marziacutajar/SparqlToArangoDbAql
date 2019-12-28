package com.aql.querytree.expressions.functions;

import com.aql.querytree.AqlConstants;
import com.aql.querytree.expressions.Expr;

public class Expr_NotEquals extends ExprFunction2 {
    private static final String functionName = "neq";
    private static final String symbol = AqlConstants.SYM_NEQ;

    public Expr_NotEquals(Expr left, Expr right)
    {
        super(left, right, functionName, symbol);
    }

    @Override
    public Expr copy(Expr e1, Expr e2) {  return new Expr_NotEquals(e1 , e2); }

}
