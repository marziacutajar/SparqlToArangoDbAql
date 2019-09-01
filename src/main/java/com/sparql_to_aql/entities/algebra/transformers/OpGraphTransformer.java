package com.sparql_to_aql.entities.algebra.transformers;

import com.sparql_to_aql.entities.algebra.OpGraphBGP;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpGraph;

public class OpGraphTransformer extends TransformCopy {

    @Override
    public Op transform(OpGraph opGraph, Op subOp) {
        if(!(subOp instanceof OpBGP)){
            return opGraph;
        }

        OpBGP p = (OpBGP) subOp;

        return new OpGraphBGP(opGraph.getNode(), p.getPattern());
    }
}
