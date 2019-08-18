package com.aql.algebra.operators;

import com.aql.algebra.OpVisitor;

//TODO choose between using this op or using Op0Nesting/Op1Nesting...
// personally I prefer the latter cause nesting isn't really an Op per se..
public class OpNest extends Op2{

    public OpNest(Op outerOp, Op nestedOps){
        super(outerOp, nestedOps);
    }

    //This would take
    @Override
    public Op2 copy(Op outerOp, Op nestedOps){
        return new OpNest(outerOp, nestedOps);
    }

    @Override
    public void visit(OpVisitor opVisitor) { opVisitor.visit(this); }

    @Override
    public String getName() { return "nest"; }


}
