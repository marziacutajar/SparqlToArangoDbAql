package com.aql.algebra.operators;

import com.aql.algebra.AqlQueryNode;

public abstract class OpModifier extends Op1 {
    public OpModifier(AqlQueryNode subOp)
    {
        super(subOp);
    }
}
