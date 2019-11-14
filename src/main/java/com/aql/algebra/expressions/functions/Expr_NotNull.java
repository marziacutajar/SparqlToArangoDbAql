package com.aql.algebra.expressions.functions;

import com.aql.algebra.expressions.Expr;
import com.aql.algebra.expressions.ExprList;

public class Expr_NotNull extends ExprFunctionN {
    private static final String functionName = "not_null";

    public Expr_NotNull(Expr... exprs)
    {
        super(functionName, exprs);
    }

    public Expr_NotNull(ExprList exprs)
    {
        super(functionName, exprs);
    }

    @Override
    public Expr_NotNull copy(Expr... exprs) {  return new Expr_NotNull(exprs); }
}
