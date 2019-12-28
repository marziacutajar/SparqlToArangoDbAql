package com.aql.querytree.resources;

import com.aql.querytree.AqlConstants;
import com.aql.querytree.NodeVisitor;
import com.aql.querytree.expressions.Expr;
import com.aql.querytree.expressions.Var;

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

    @Override
    public String getName() { return AqlConstants.keywordFor; }

    public Var getIterationVar() { return iterationVariable; }

    public Expr getDataArrayExpr() { return dataArrayExpr; }

    @Override
    public void visit(NodeVisitor resVisitor) { resVisitor.visit(this); }
}
