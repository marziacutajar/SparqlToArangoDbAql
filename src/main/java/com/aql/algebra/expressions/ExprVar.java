package com.aql.algebra.expressions;

import com.aql.algebra.ExprVisitor;

public class ExprVar extends Expr{
    protected Var var = null;

    public ExprVar(String name) { var = Var.alloc(name); }

    public ExprVar(Var v)
    {
        var = v;
    }

    //public Expr copy(Var v)  { return new ExprVar(v); }

    @Override
    public void visit(ExprVisitor visitor) { visitor.visit(this); }

    /*public Expr apply(ExprTransform transform)  {
        if ( transform == null )
            throw new NullPointerException();
        return transform.transform(this); }
    */

    /** @return Returns the name. */
    @Override
    public String getVarName()  { return var.getVarName(); }

    @Override
    public ExprVar getExprVar() { return this; }

    @Override
    public String toString()        { return var.toString(); }
}
