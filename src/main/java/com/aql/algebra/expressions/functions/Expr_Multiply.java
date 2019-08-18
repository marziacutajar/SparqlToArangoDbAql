package com.aql.algebra.expressions.functions;

import com.aql.algebra.AqlConstants;
import com.aql.algebra.expressions.Expr;

public class Expr_Multiply extends ExprFunction2 {
    private static final String functionName = "multiply";
    private static final String symbol = AqlConstants.SYM_MULTIPLY;

    public Expr_Multiply(Expr left, Expr right)
    {
        super(left, right, functionName, symbol);
    }

    @Override
    public Expr copy(Expr e1, Expr e2) {  return new Expr_Multiply(e1 , e2); }

}
