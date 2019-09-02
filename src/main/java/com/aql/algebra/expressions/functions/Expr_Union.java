package com.aql.algebra.expressions.functions;

import com.aql.algebra.expressions.Expr;

public class Expr_Union extends ExprFunctionN {
    private static final String functionName = "union";

    public Expr_Union(Expr... exprs)
    {
        super(functionName, exprs);
    }

    @Override
    public Expr_Union copy(Expr... exprs) {  return new Expr_Union(exprs); }

}
