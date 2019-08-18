package com.sparql_to_aql.entities.algebra.aql.expressions.functions;

import com.sparql_to_aql.entities.algebra.aql.expressions.Expr;

public class Expr_Conditional extends ExprFunction3 {
    private static final String functionName = "if_then_else";

    private final Expr condition;
    private final Expr thenExpr;
    private final Expr elseExpr;

    public Expr_Conditional(Expr condition, Expr thenExpr, Expr elseExpr)
    {
        super(condition, thenExpr, elseExpr, functionName);
        // Better names for the expressions
        this.condition = condition;
        this.thenExpr = thenExpr;
        this.elseExpr = elseExpr;
    }

    @Override
    public Expr copy(Expr arg1, Expr arg2, Expr arg3)
    {
        return new Expr_Conditional(arg1, arg2, arg3);
    }

}
