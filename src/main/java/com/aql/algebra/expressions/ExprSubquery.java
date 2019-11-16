package com.aql.algebra.expressions;

import com.aql.algebra.ExprVisitor;
import com.aql.algebra.operators.Op;

public class ExprSubquery extends Expr{
    private final Op op ;

    public ExprSubquery(Op op)
    {
        super() ;
        this.op = op ;
    }

    @Override
    public boolean isSubquery() { return true; }

    @Override
    public Op getSubquery() { return op; }

    @Override
    public void visit(ExprVisitor visitor) { visitor.visit(this); }

    @Override
    public String toString()
    {
        return null;
    }
}
