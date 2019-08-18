package com.aql.algebra.operators;

import java.util.ArrayList;
import java.util.List;

public abstract class OpNesting implements Op{
    private List<Op> nestedOps;

    public OpNesting()
    {
        this.nestedOps = new ArrayList<>();
    }

    //TODO all OP implementations extend OpNesting... override the below methods in each Op so we don't allow nesting ops
    // in ops that don't support nesting ex. Project
    public void addNestedOp(Op nestedOp){
        nestedOps.add(nestedOp);
    }

    public void addNestedOps(List<? extends Op> nestedOps){
        this.nestedOps.addAll(nestedOps);
    }

    /*public OpNesting(List<Op> nestedOps)
    {
        this.nestedOps = nestedOps;
    }*/
}
