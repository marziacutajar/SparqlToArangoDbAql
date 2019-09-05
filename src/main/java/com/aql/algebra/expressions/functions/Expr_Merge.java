package com.aql.algebra.expressions.functions;

import com.aql.algebra.expressions.Expr;

public class Expr_Merge extends ExprFunctionN {
    private static final String functionName = "merge";

    public Expr_Merge(Expr... exprs)
    {
        super(functionName, exprs);
    }

    @Override
    public Expr_Merge copy(Expr... exprs) {  return new Expr_Merge(exprs); }
}
