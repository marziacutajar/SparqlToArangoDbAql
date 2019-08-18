package com.sparql_to_aql.entities.algebra.aql.operators;

import com.sparql_to_aql.entities.algebra.aql.AqlConstants;
import com.sparql_to_aql.entities.algebra.aql.OpVisitor;

//TODO remove this if we're going to use OpNest or Op0Nesting/Op1Nesting
public class OpExtend extends Op1{
    //TODO consider: difference between this Op and OpAssign would be that OpExtend is used for nested LETs and OpAssign
    // is used to assign the whole subOp passed to it into a variable

    public OpExtend(Op sub) {
        super(sub);
    }

    @Override
    public String getName() { return "extend"; }

    @Override
    public void visit(OpVisitor opVisitor) { opVisitor.visit(this); }

    @Override
    public Op1 copy(Op subOp){
        return new OpExtend(subOp);
    }

}
