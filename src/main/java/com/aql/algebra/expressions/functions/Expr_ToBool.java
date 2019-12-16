package com.aql.algebra.expressions.functions;

import com.aql.algebra.expressions.Expr;

public class Expr_ToBool extends ExprFunction1 {
    private static final String functionName = "to_bool";

    public Expr_ToBool(Expr expr)
    {
        super(expr, functionName);
    }

    @Override
    public Expr copy(Expr expr) { return new Expr_ToBool(expr); }
}