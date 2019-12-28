package com.aql.querytree.resources;

import com.aql.querytree.AqlQueryNode;
import com.aql.querytree.NodeVisitor;

public interface Resource extends AqlQueryNode
{
    void visit(NodeVisitor resVisitor);
}
