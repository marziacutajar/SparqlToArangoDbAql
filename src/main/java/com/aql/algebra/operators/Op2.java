package com.aql.algebra.operators;

public abstract class Op2 implements Op{
    private Op left ;
    private Op right ;

    public Op2(Op left, Op right)
    {
        this.left = left ; this.right = right ;
    }

    public Op getLeft() { return left ; }
    public Op getRight() { return right ; }

    //public abstract Op apply(Transform transform, Op left, Op right) ;
    public abstract Op2 copy(Op left, Op right) ;

    @Override
    public int hashCode()
    {
        return left.hashCode()<<1 ^ right.hashCode() ^ getName().hashCode() ;
    }
}
