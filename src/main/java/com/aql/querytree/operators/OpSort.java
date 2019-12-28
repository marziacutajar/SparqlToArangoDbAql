package com.aql.querytree.operators;

import com.aql.querytree.AqlConstants;
import com.aql.querytree.AqlQueryNode;
import com.aql.querytree.NodeVisitor;
import com.aql.querytree.SortCondition;

import java.util.List;

public class OpSort extends OpModifier
{
    private List<SortCondition> conditions;

    public OpSort(AqlQueryNode subOp, List<SortCondition> conditions)
    {
        super(subOp);
        this.conditions = conditions;
    }

    public List<SortCondition> getConditions() { return conditions; }

    @Override
    public String getName() { return AqlConstants.keywordSort; }

    @Override
    public void visit(NodeVisitor opVisitor) { opVisitor.visit(this); }

    @Override
    public Op1 copy(AqlQueryNode subOp) { return new OpSort(subOp, conditions); }

}
