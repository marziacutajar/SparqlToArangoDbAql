package com.sparql_to_aql.entities.algebra.transformers;

import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpGraph;

public class OpGraphTransformer extends TransformCopy {

    @Override
    public Op transform(OpGraph opGraph, Op subOp) {
        return Algebra.toQuadForm(opGraph);
    }
}
