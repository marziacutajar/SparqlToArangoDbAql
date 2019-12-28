package com.aql.querytree.operators;

import com.aql.querytree.AqlQueryNode;

public abstract class Op2 implements Op{
    private AqlQueryNode left;
    private AqlQueryNode right;

    public Op2(AqlQueryNode left, AqlQueryNode right)
    {
        this.left = left;
        this.right = right;
    }

    public AqlQueryNode getLeft() { return left; }
    public AqlQueryNode getRight() { return right; }

    public abstract Op2 copy(Op left, Op right);

    @Override
    public int hashCode()
    {
        return left.hashCode()<<1 ^ right.hashCode() ^ getName().hashCode();
    }
}
