package com.aql.algebra.operators;

public abstract class Op0 extends OpNesting implements Op {
    //public abstract Op apply(Transform transform, Op subOp);
    public abstract Op0 copy();
}
