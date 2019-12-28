package com.aql.querytree.expressions.functions;

import com.aql.querytree.expressions.Expr;

public class Expr_In extends ExprFunction2 {
    private static final String functionName = "in";

    public Expr_In(Expr left, Expr right)
    {
        super(left, right, functionName);
    }

    @Override
    public Expr copy(Expr arg1, Expr arg2)
    {
        return new Expr_In(arg1, arg2);
    }

}
