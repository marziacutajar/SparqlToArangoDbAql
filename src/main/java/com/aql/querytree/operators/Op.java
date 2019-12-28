package com.aql.querytree.operators;

import com.aql.querytree.AqlQueryNode;

public interface Op extends AqlQueryNode
{
    String getName();
}
