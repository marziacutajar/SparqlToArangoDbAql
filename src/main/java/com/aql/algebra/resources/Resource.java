package com.aql.algebra.resources;

import com.aql.algebra.AqlQueryNode;
import com.aql.algebra.NodeVisitor;

public interface Resource extends AqlQueryNode
{
    void visit(NodeVisitor resVisitor);
}
