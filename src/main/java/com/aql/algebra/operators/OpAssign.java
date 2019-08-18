package com.sparql_to_aql.entities.algebra.aql.operators;

import com.sparql_to_aql.entities.algebra.aql.AqlConstants;
import com.sparql_to_aql.entities.algebra.aql.OpVisitor;
import com.sparql_to_aql.entities.algebra.aql.expressions.Expr;

public class OpAssign extends Op0{
    //can assign either an expression or an Op result to a variable
    private String variableName;
    private Expr exprValue;
    private Op opValue;

    public OpAssign(String variableName, Expr exprValue){
        OpAssign(variableName, exprValue, null);
    }

    public OpAssign(String variableName, Op opValue){
        OpAssign(variableName, null, opValue);
    }

    private void OpAssign(String variableName, Expr exprValue, Op opValue){
        this.variableName = variableName;
        this.exprValue = exprValue;
        this.opValue = opValue;
    }

    @Override
    public String getName() { return AqlConstants.keywordLet; }

    //TODO
    @Override
    public Op0 copy(){
        return null;
    }

    @Override
    public void visit(OpVisitor opVisitor) { opVisitor.visit(this); }

}
