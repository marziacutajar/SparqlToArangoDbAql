package com.sparql_to_aql.entities.algebra.aql.operators;

import com.sparql_to_aql.entities.algebra.aql.AqlConstants;
import com.sparql_to_aql.entities.algebra.aql.OpVisitor;
import com.sparql_to_aql.entities.algebra.aql.expressions.Expr;

import java.util.ArrayList;
import java.util.List;

//TODO consider instead of having a Forloop operator, create a class called IterationResource with the below private fields.. and pass IterationResource to filter operations, extend operations, etc..
public class OpFor extends Op0Nesting {
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
