package com.aql.algebra.operators;

import com.aql.algebra.AqlConstants;
import com.aql.algebra.OpVisitor;
import com.aql.algebra.expressions.Expr;

public class OpAssign extends Op0{
    //can assign either an expression or an Op result to a variable
    private String variableName;
    private Expr exprValue;
    private Op opValue;

    boolean assigningOp;
    boolean assigningExpr;

    public OpAssign(String variableName, Expr exprValue){
        OpAssign(variableName, exprValue, null);
        assigningOp = false;
        assigningExpr = true;
    }

    public OpAssign(String variableName, Op opValue){
        OpAssign(variableName, null, opValue);
        assigningOp = true;
        assigningExpr = false;
    }

    private void OpAssign(String variableName, Expr exprValue, Op opValue){
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
    public Op0 copy(){
        if(exprValue == null)
            return new OpAssign(variableName, opValue);
        else
            return new OpAssign(variableName, exprValue);
    }

    @Override
    public void visit(OpVisitor opVisitor) { opVisitor.visit(this); }

}
