package com.aql.algebra;

public interface AqlQueryNode {
    void visit(NodeVisitor opVisitor);
}
