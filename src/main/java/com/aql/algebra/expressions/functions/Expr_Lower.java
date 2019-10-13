package com.aql.algebra.expressions.functions;

import com.aql.algebra.expressions.Expr;

public class Expr_Lower extends ExprFunction1 {
    private static final String functionName = "lower";

    public Expr_Lower(Expr expr)
    {
        super(expr, functionName);
    }

    @Override
    public Expr copy(Expr expr) { return new Expr_Lower(expr); }

}
