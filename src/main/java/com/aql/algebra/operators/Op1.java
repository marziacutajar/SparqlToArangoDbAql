package com.aql.algebra.operators;

import com.aql.algebra.AqlQueryNode;

public abstract class Op1 implements Op {
    private AqlQueryNode child;

    public Op1(AqlQueryNode child)
    {
        this.child = child;
    }

    public AqlQueryNode getChild() { return child; }

    //public void setSubOp(Op op) { child = op; }
    //public abstract Op apply(Transform transform, Op subOp);

    public abstract Op1 copy(AqlQueryNode child);
}
