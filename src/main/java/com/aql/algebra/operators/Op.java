package com.aql.algebra.operators;

import com.aql.algebra.AqlQueryNode;

public interface Op extends AqlQueryNode
{
    public String getName();
    //public boolean equalTo(Op other, NodeIsomorphismMap labelMap);
}
