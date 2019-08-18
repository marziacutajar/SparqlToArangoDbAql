package com.aql.algebra.expressions.functions;

import com.aql.algebra.AqlConstants;
import com.aql.algebra.expressions.Expr;

public class Expr_Length extends ExprFunction1{
    private static final String symbol = AqlConstants.SYM_AND;

    public Expr_Length(Expr expr)
    {
        super(expr, symbol);
    }

    @Override
    public Expr copy(Expr expr) {  return new Expr_Length(expr); }

}
