package com.sparql_to_aql.entities.algebra.transformers;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpSequence;

import java.util.List;

public class OpSequenceTransformer extends TransformCopy {
    @Override
    public Op transform(OpSequence opSequence, List<Op> elts) {
        Op newOp = null;
        boolean isFirst = true;
        for(Op op: elts){
            if(isFirst){
                newOp = op;
                isFirst = false;
            }
            else{
                newOp = OpJoin.create(newOp, op);
            }
        }

        return newOp;
    }
}
