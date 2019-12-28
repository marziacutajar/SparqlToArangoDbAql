package com.aql.querytree.expressions.functions;

import com.aql.querytree.expressions.Expr;

public class Expr_Length extends ExprFunction1{
    private static final String functionName = "length";

    public Expr_Length(Expr expr)
    {
        super(expr, functionName);
    }

    @Override
    public Expr copy(Expr expr) {  return new Expr_Length(expr); }

}
