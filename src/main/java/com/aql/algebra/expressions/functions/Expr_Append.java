package com.aql.algebra.expressions.functions;

import com.aql.algebra.expressions.Expr;

public class Expr_Append extends ExprFunction3 {
    private static final String functionName = "append";

    private final Expr array;
    private final Expr values;
    private final Expr unique;

    public Expr_Append(Expr array, Expr values, Expr unique)
    {
        super(array, values, unique, functionName);
        // Better names for the expressions
        this.array = array;
        this.values = values;
        this.unique = unique;
    }

    @Override
    public Expr copy(Expr arg1, Expr arg2, Expr arg3)
    {
        return new Expr_Append(arg1, arg2, arg3);
    }
}
