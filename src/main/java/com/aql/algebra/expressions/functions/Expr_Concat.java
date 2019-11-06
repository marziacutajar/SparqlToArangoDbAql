package com.aql.algebra.expressions.functions;

import com.aql.algebra.expressions.Expr;

public class Expr_Concat extends ExprFunctionN {
    private static final String functionName = "concat";

    public Expr_Concat(Expr... exprs)
    {
        super(functionName, exprs);
    }

    @Override
    public Expr_Concat copy(Expr... exprs) {  return new Expr_Concat(exprs); }
}
