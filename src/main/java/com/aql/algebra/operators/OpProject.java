package com.aql.algebra.operators;

import com.aql.algebra.AqlConstants;
import com.aql.algebra.AqlQueryNode;
import com.aql.algebra.NodeVisitor;
import com.aql.algebra.expressions.Expr;

import java.util.ArrayList;
import java.util.List;

public class OpProject extends OpModifier {

    private List<Expr> exprs = new ArrayList<>();

    private boolean distinct;

    public OpProject(AqlQueryNode subOp, List<Expr> exprs, boolean distinct)
    {
        super(subOp);
        this.exprs = exprs;
        this.distinct = distinct;
    }

    public OpProject(AqlQueryNode subOp, Expr expr, boolean distinct)
    {
        super(subOp);
        this.exprs.add(expr);
        this.distinct = distinct;
    }

    public List<Expr> getExprs() { return exprs; }

    public boolean isDistinct() { return distinct; }

    @Override
    public String getName() { return AqlConstants.keywordReturn; }

    @Override
    public void visit(NodeVisitor opVisitor)  { opVisitor.visit(this); }

    @Override
    public Op1 copy(AqlQueryNode subOp)                { return new OpProject(subOp, exprs, distinct); }

    /*@Override
    public Op apply(Transform transform, Op subOp)
    { return transform.transform(this, subOp); }*/

    /*@Override
    public int hashCode()
    {
        return exprs.hashCode() ^ getSubOp().hashCode();
    }*/
}
