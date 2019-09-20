package com.aql.algebra.operators;

import com.aql.algebra.AqlQueryNode;
import com.aql.algebra.NodeVisitor;

//op used to nest for loops
public class OpNest extends Op2{

    public OpNest(AqlQueryNode outerOp, AqlQueryNode nestedOps){
        super(outerOp, nestedOps);
    }

    //This would take
    @Override
    public Op2 copy(Op outerOp, Op nestedOps){
        return new OpNest(outerOp, nestedOps);
    }

    @Override
    public void visit(NodeVisitor opVisitor) { opVisitor.visit(this); }

    @Override
    public String getName() { return "nest"; }


}
