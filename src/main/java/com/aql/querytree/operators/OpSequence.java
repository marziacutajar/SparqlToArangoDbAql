package com.aql.querytree.operators;

import com.aql.querytree.AqlQueryNode;
import com.aql.querytree.NodeVisitor;
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
