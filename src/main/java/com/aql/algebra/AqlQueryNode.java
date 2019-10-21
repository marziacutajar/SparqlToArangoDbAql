package com.aql.algebra;

public interface AqlQueryNode extends Named{
    void visit(NodeVisitor opVisitor);
}
