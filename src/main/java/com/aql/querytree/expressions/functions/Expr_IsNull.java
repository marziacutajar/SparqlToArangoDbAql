package com.aql.querytree.expressions.functions;

import com.aql.querytree.expressions.Expr;

public class Expr_IsNull extends ExprFunction1 {
    private static final String functionName = "is_null";

    public Expr_IsNull(Expr expr)
    {
        super(expr, functionName);
    }

    @Override
    public Expr_IsNull copy(Expr expr) { return new Expr_IsNull(expr); }
}
