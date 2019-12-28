package com.aql.querytree.operators;

import com.aql.querytree.AqlConstants;
import com.aql.querytree.AqlQueryNode;
import com.aql.querytree.NodeVisitor;

public class OpLimit extends OpModifier
{
    private long start;
    private long length;

    public OpLimit(AqlQueryNode subOp, long start, long length)
    {
        super(subOp);
        this.start = start;
        this.length = length;
    }

    public long getLength() { return length; }

    public long getStart() { return start; }

    public Op copy()
    {
        return null;
    }

    @Override
    public String getName() { return AqlConstants.keywordLimit; }

    @Override
    public void visit(NodeVisitor opVisitor) { opVisitor.visit(this); }

    @Override
    public Op1 copy(AqlQueryNode subOp) { return new OpLimit(subOp, start, length); }
}
