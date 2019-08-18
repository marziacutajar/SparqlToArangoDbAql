package com.sparql_to_aql.entities.algebra.aql.operators;

//TODO consider extending Op1Nesting here
public abstract class OpModifier extends Op1 {

    public static Op removeModifiers(Op op)
    {
        while( op instanceof OpModifier )
            op = ((OpModifier)op).getSubOp();
        return op;
    }

    public OpModifier(Op subOp)
    {
        super(subOp);
    }
}