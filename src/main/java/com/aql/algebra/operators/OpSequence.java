package com.aql.algebra.operators;

import com.aql.algebra.OpVisitor;

import java.util.List;

public class OpSequence extends OpN {
    public OpSequence(List<Op> ops){
        super(ops);
    }

    @Override
    public OpN copy(List<Op> ops){
        return new OpSequence(ops);
    }

    @Override
    public void visit(OpVisitor opVisitor) { opVisitor.visit(this); }

    @Override
    public String getName() { return "seq"; }

}
