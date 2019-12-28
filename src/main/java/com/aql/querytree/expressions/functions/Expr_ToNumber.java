package com.aql.querytree.expressions.functions;

import com.aql.querytree.expressions.Expr;

public class Expr_ToNumber extends ExprFunction1 {
    private static final String functionName = "to_number";

    public Expr_ToNumber(Expr expr)
    {
        super(expr, functionName);
    }

    @Override
    public Expr copy(Expr expr) { return new Expr_ToNumber(expr); }
}
