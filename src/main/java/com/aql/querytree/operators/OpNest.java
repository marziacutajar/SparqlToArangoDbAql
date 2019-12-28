package com.aql.querytree.operators;

import com.aql.querytree.AqlQueryNode;
import com.aql.querytree.NodeVisitor;

//op used to nest for loops
public class OpNest extends Op2{

    public OpNest(AqlQueryNode outerOp, AqlQueryNode nestedOps){
        super(outerOp, nestedOps);
    }

    @Override
    public Op2 copy(Op outerOp, Op nestedOps){
        return new OpNest(outerOp, nestedOps);
    }

    @Override
    public void visit(NodeVisitor opVisitor) { opVisitor.visit(this); }

    @Override
    public String getName() { return "nest"; }
}
