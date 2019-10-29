package com.aql.algebra.operators;

import com.aql.algebra.AqlQueryNode;
import com.aql.algebra.NodeVisitor;

import java.util.List;

public class OpSequence extends OpN {

    public OpSequence(){
        super();
    }

    public OpSequence(List<AqlQueryNode> ops){
        super(ops);
    }

    @Override
    public OpN copy(List<AqlQueryNode> ops){
        return new OpSequence(ops);
    }

    @Override
    public void visit(NodeVisitor opVisitor) { opVisitor.visit(this); }

    @Override
    public String getName() { return "seq"; }

}
