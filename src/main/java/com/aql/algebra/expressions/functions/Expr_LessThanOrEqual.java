package com.aql.algebra.expressions.functions;

import com.aql.algebra.AqlConstants;
import com.aql.algebra.expressions.Expr;

public class Expr_LessThanOrEqual extends ExprFunction2 {
    private static final String functionName = "lte";
    private static final String symbol = AqlConstants.SYM_LTE;

    public Expr_LessThanOrEqual(Expr left, Expr right)
    {
        super(left, right, functionName, symbol);
    }

    @Override
    public Expr copy(Expr e1, Expr e2) {  return new Expr_LessThanOrEqual(e1 , e2); }

}
