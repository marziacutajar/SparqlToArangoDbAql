package com.aql.querytree.expressions.functions;

import com.aql.querytree.expressions.Expr;
import com.aql.querytree.expressions.ExprList;

public class Expr_Concat extends ExprFunctionN {
    private static final String functionName = "concat";

    public Expr_Concat(Expr... exprs)
    {
        super(functionName, exprs);
    }

    public Expr_Concat(ExprList exprs)
    {
        super(functionName, exprs);
    }

    @Override
    public Expr_Concat copy(Expr... exprs) {  return new Expr_Concat(exprs); }
}
