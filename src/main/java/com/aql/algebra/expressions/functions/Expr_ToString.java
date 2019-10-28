package com.aql.algebra.expressions.functions;

import com.aql.algebra.expressions.Expr;

public class Expr_ToString extends ExprFunction1 {
    private static final String functionName = "to_string";

    public Expr_ToString(Expr expr)
    {
        super(expr, functionName);
    }

    @Override
    public Expr copy(Expr expr) { return new Expr_ToNumber(expr); }
}