package com.aql.algebra.operators;

import com.aql.algebra.AqlConstants;
import com.aql.algebra.AqlQueryNode;
import com.aql.algebra.NodeVisitor;
import com.aql.algebra.expressions.ExprAggregator;
import com.aql.algebra.expressions.VarExprList;

import java.util.List;

public class OpCollect extends Op1
{
    private VarExprList groupVars;
    private List<ExprAggregator> aggregators;

    public static OpCollect create(AqlQueryNode subOp, VarExprList groupVars, List<ExprAggregator> aggregators) {
        return new OpCollect(subOp, groupVars, aggregators);
    }

    public OpCollect(AqlQueryNode subOp, VarExprList groupVars, List<ExprAggregator> aggregators)
    {
        super(subOp);
        this.groupVars  = groupVars;
        this.aggregators = aggregators;
    }

    @Override
    public String getName()                     { return AqlConstants.keywordCollect; }
    public VarExprList getGroupVars()           { return groupVars; }
    public List<ExprAggregator> getAggregators()  { return aggregators; }

    @Override
    public void visit(NodeVisitor opVisitor)      { opVisitor.visit(this); }

    @Override
    public Op1 copy(AqlQueryNode child)                    { return new OpCollect(child, groupVars, aggregators); }

    /*@Override
    public Op apply(Transform transform, Op subOp)
    { return transform.transform(this, subOp); }

    @Override
    public int hashCode()
    {
        int x = getSubOp().hashCode();
        if ( groupVars != null )
            x ^= groupVars.hashCode();
        if ( aggregators != null )
            x ^= aggregators.hashCode();
        return x;
    }

    @Override
    public boolean equalTo(Op other, NodeIsomorphismMap labelMap)
    {
        if ( ! (other instanceof OpGroup) ) return false;
        OpGroup opGroup = (OpGroup)other;
        if ( ! Objects.equals(groupVars, opGroup.groupVars) )
            return false;
        if ( ! Objects.equals(aggregators, opGroup.aggregators) )
            return false;

        return getSubOp().equalTo(opGroup.getSubOp(), labelMap);
    }*/
}
