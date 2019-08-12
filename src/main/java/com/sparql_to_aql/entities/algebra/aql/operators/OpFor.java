package com.sparql_to_aql.entities.algebra.aql.operators;

import com.sparql_to_aql.entities.algebra.aql.AqlConstants;
import com.sparql_to_aql.entities.algebra.aql.OpVisitor;

import java.util.ArrayList;
import java.util.List;

public class OpFor implements Op {
    //TODO consider changing this to Var type..or remove Var type completely!!
    private String iterationVariable;

    private String dataArrayName;

    //TODO possibly keep list of nested subqueries/assignments here
    private List<Op> subqueriesOrAssignments;

    public OpFor(String iterationVarName, String dataArrayName)
    {
        this.iterationVariable = iterationVarName;
        this.dataArrayName = dataArrayName;
        this.subqueriesOrAssignments = new ArrayList<>();
    }

    public String getIterationVar() { return iterationVariable; }

    public String getDataArrayName() { return dataArrayName; }

    @Override
    public void visit(OpVisitor opVisitor) { opVisitor.visit(this); }

    @Override
    public String getName() { return AqlConstants.keywordFor; }

    public Op addSubqueryOrAssignment(Op subqueryIn){
        subqueriesOrAssignments.add(subqueryIn);
        return this;
    }
}
