package com.aql.querytree.operators;

import com.aql.querytree.AqlQueryNode;

public abstract class OpModifier extends Op1 {
    public OpModifier(AqlQueryNode subOp)
    {
        super(subOp);
    }
}
