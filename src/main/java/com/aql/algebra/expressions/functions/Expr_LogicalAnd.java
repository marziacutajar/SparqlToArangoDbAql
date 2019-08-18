package com.aql.algebra.expressions.functions;

import com.aql.algebra.AqlConstants;
import com.aql.algebra.expressions.Expr;

public class Expr_LogicalAnd extends ExprFunction2 {
    private static final String functionName = "and";
    private static final String symbol = AqlConstants.SYM_AND;

    public Expr_LogicalAnd(Expr left, Expr right)
    {
        super(left, right, functionName, symbol);
    }

    @Override
    public Expr copy(Expr e1, Expr e2) {  return new Expr_LogicalAnd(e1 , e2); }

}
