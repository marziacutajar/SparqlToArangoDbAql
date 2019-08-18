package com.aql.algebra.expressions.functions;

import com.aql.algebra.AqlConstants;
import com.aql.algebra.expressions.Expr;

public class Expr_GreaterThan extends ExprFunction2 {
    private static final String functionName = "gt";
    private static final String symbol = AqlConstants.SYM_GT;

    public Expr_GreaterThan(Expr left, Expr right)
    {
        super(left, right, functionName, symbol);
    }

    @Override
    public Expr copy(Expr e1, Expr e2) {  return new Expr_GreaterThan(e1 , e2); }
}
