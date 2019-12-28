package com.aql.querytree.expressions.functions;

import com.aql.querytree.expressions.Expr;
import com.aql.querytree.expressions.ExprFunction;

public abstract class ExprFunction0 extends ExprFunction {

    protected ExprFunction0(String fName) { this(fName, null); }

    protected ExprFunction0(String fName, String opSign)
    {
        super(fName, opSign);
    }

    @Override
    public Expr getArg(int i)       { return null; }

    @Override
    public int numArgs()            { return 0; }

    public abstract Expr copy();

    /*@Override
    public void visit(ExprVisitor visitor) { visitor.visit(this); }*/
}
