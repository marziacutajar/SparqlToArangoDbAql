package com.aql.algebra.operators;

import com.aql.algebra.AqlConstants;
import com.aql.algebra.AqlQueryNode;
import com.aql.algebra.NodeVisitor;
import com.aql.algebra.SortCondition;

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
    public String getName()                 { return AqlConstants.keywordSort; }

    @Override
    public void visit(NodeVisitor opVisitor)  { opVisitor.visit(this); }

    @Override
    public Op1 copy(AqlQueryNode subOp)                { return new OpSort(subOp, conditions); }

    /*@Override
    public Op apply(Transform transform, Op subOp)
    { return transform.transform(this, subOp); }

    @Override
    public int hashCode()
    {
        return conditions.hashCode() ^ getSubOp().hashCode();
    }*/
}
