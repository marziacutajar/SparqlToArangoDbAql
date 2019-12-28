package com.aql.querytree.expressions.functions;

import com.aql.querytree.ExprVisitor;
import com.aql.querytree.expressions.Expr;
import com.aql.querytree.expressions.ExprFunction;

public abstract class ExprFunction1 extends ExprFunction {
    protected final Expr expr;

    protected ExprFunction1(Expr expr, String fName) { this(expr, fName, null); }

    protected ExprFunction1(Expr expr, String fName, String opSign)
    {
        super(fName, opSign);
        this.expr = expr;
    }

    public Expr getArg() { return expr; }

    @Override
    public Expr getArg(int i)
    {
        if ( i == 1 )
            return expr;
        return null;
    }

    @Override
    public int numArgs() { return 1; }

    public abstract Expr copy(Expr expr);

    @Override
    public void visit(ExprVisitor visitor) { visitor.visit(this); }
}
