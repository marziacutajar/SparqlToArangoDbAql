package com.aql.querytree.expressions;

import com.aql.querytree.ExprVisitor;

public class ExprVar extends Expr {
    protected Var var;

    public ExprVar(String name) { var = Var.alloc(name); }

    public ExprVar(Var v)
    {
        var = v;
    }

    //public Expr copy(Var v)  { return new ExprVar(v); }

    @Override
    public void visit(ExprVisitor visitor) { visitor.visit(this); }

    /** @return Returns the name. */
    @Override
    public String getVarName() { return var.getVarName(); }

    @Override
    public ExprVar getExprVar() { return this; }

    @Override
    public String toString() { return var.toString(); }
}
