package com.aql.algebra.operators;

import com.aql.algebra.AqlConstants;
import com.aql.algebra.OpVisitor;
import com.aql.algebra.expressions.Expr;

//TODO consider instead of having a Forloop operator, create a class called IterationResource with the below private fields.. and pass IterationResource to filter operations, extend operations, etc..
public class OpFor extends Op0 {
    //TODO consider changing this to Var type..or remove Var type completely!!
    private String iterationVariable;

    private Expr dataArrayExpr;

    public OpFor(String iterationVarName, Expr dataArrayExpr)
    {
        this.iterationVariable = iterationVarName;
        this.dataArrayExpr = dataArrayExpr;
    }

    public String getIterationVar() { return iterationVariable; }

    public Expr getDataArrayExpr() { return dataArrayExpr; }

    @Override
    public void visit(OpVisitor opVisitor) { opVisitor.visit(this); }

    @Override
    public String getName() { return AqlConstants.keywordFor; }

    @Override
    public Op0 copy(){
        return new OpFor(iterationVariable, dataArrayExpr);
    }

}
