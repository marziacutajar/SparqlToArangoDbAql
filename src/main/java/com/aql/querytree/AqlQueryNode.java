package com.aql.querytree;

public interface AqlQueryNode extends Named{
    void visit(NodeVisitor opVisitor);
}
