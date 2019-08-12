package com.sparql_to_aql.entities.algebra.aql.operators;

import com.sparql_to_aql.entities.algebra.aql.OpVisitor;

public interface Op
{
    public void visit(OpVisitor opVisitor);
    public String getName();
    //public boolean equalTo(Op other, NodeIsomorphismMap labelMap);
}
