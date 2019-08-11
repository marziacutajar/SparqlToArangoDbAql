package com.sparql_to_aql.entities.algebra.aql;

public interface Op
{
    public void visit(OpVisitor opVisitor);
    public String getName();
    //public boolean equalTo(Op other, NodeIsomorphismMap labelMap) ;
}
