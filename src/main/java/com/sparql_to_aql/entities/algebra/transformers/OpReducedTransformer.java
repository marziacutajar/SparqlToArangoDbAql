package com.sparql_to_aql.entities.algebra.transformers;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpReduced;

public class OpReducedTransformer extends TransformCopy {
    @Override
    public Op transform(OpReduced opReduced, Op subOp) {
       return subOp;
    }
}
