package com.aql.algebra.expressions.functions;

import com.aql.algebra.AqlConstants;
import com.aql.algebra.expressions.Expr;

public class Expr_Divide extends ExprFunction2{
    private static final String functionName = "divide";
    private static final String symbol = AqlConstants.SYM_DIVIDE;

    public Expr_Divide(Expr left, Expr right)
    {
        super(left, right, functionName, symbol);
    }

    @Override
    public Expr copy(Expr e1, Expr e2) {  return new Expr_Divide(e1 , e2); }

}
