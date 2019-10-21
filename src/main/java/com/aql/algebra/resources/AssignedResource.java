package com.aql.algebra.resources;

import com.aql.algebra.AqlConstants;
import com.aql.algebra.NodeVisitor;
import com.aql.algebra.expressions.Expr;
import com.aql.algebra.operators.Op;

public class AssignedResource implements Resource{
    //can assign either an expression or an Op result to a variable
    private String variableName;
    private Expr exprValue;
    private Op opValue;

    boolean assigningOp;
    boolean assigningExpr;

    public AssignedResource(String variableName, Expr exprValue){
        AssignedResource(variableName, exprValue, null);
        assigningOp = false;
        assigningExpr = true;
    }

    public AssignedResource(String variableName, Op opValue){
        AssignedResource(variableName, null, opValue);
        assigningOp = true;
        assigningExpr = false;
    }

    private void AssignedResource(String variableName, Expr exprValue, Op opValue){
        this.variableName = variableName;
        this.exprValue = exprValue;
        this.opValue = opValue;
    }

    @Override
    public String getName() { return AqlConstants.keywordLet; }

    public String getVariableName() { return variableName; }

    public Expr getExpr() { return exprValue; }

    public Op getOp() { return opValue; }

    public boolean assignsOp(){ return assigningOp; }

    public boolean assignsExpr(){ return assigningExpr; }

    @Override
    public void visit(NodeVisitor resVisitor) { resVisitor.visit(this); }
}
