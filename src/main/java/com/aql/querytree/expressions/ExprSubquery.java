package com.aql.querytree.expressions;

import com.aql.querytree.ExprVisitor;
import com.aql.querytree.operators.Op;

public class ExprSubquery extends Expr{
    private final Op op;

    public ExprSubquery(Op op)
    {
        super();
        this.op = op;
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
