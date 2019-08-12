package com.sparql_to_aql.entities.algebra.aql.operators;

public abstract class Op1 implements Op {
    private Op sub;

    public Op1(Op subOp)
    {
        this.sub = subOp;
    }

    public Op getSubOp() { return sub; }
    //public void setSubOp(Op op) { sub = op; }

    //public abstract Op apply(Transform transform, Op subOp);
    public abstract Op1 copy(Op subOp);
}
