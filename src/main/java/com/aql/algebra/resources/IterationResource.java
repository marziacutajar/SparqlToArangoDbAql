package com.aql.algebra.resources;

import com.aql.algebra.NodeVisitor;
import com.aql.algebra.expressions.Expr;
import com.aql.algebra.expressions.Var;

public class IterationResource implements Resource{
    private Var iterationVariable;
    private Expr dataArrayExpr;

    public IterationResource(String iterationVarName, Expr dataArrayExpr)
    {
        this.iterationVariable = Var.alloc(iterationVarName);
        this.dataArrayExpr = dataArrayExpr;
    }

    public IterationResource(Var iterationVar, Expr dataArrayExpr)
    {
        this.iterationVariable = iterationVar;
        this.dataArrayExpr = dataArrayExpr;
    }

    public Var getIterationVar() { return iterationVariable; }

    public Expr getDataArrayExpr() { return dataArrayExpr; }

    @Override
    public void visit(NodeVisitor resVisitor) { resVisitor.visit(this); }
}
