package com.aql.querytree.expressions.functions;

import com.aql.querytree.expressions.Expr;
import com.aql.querytree.expressions.ExprList;

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
