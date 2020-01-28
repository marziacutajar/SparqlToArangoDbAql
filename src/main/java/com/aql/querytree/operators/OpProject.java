package com.aql.querytree.operators;

import com.aql.querytree.AqlConstants;
import com.aql.querytree.AqlQueryNode;
import com.aql.querytree.NodeVisitor;
import com.aql.querytree.expressions.Expr;

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
    public void visit(NodeVisitor opVisitor) { opVisitor.visit(this); }

    @Override
    public Op1 copy(AqlQueryNode subOp) { return new OpProject(subOp, exprs, distinct); }

}
