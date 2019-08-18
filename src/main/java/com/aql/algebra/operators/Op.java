package com.aql.algebra.operators;

import com.aql.algebra.OpVisitor;

public interface Op
{
    public void visit(OpVisitor opVisitor);
    public String getName();
    //public boolean equalTo(Op other, NodeIsomorphismMap labelMap);
}
