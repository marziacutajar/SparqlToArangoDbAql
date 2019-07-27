package com.sparql_to_aql.entities.algebra.transformers;

import com.sparql_to_aql.entities.algebra.OpDistinctProject;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpDistinct;
import org.apache.jena.sparql.algebra.op.OpProject;

//IMP: Extending TransformCopy, not TransformBase as the latter doesn't transform a tree
public class OpDistinctTransformer extends TransformCopy {

    @Override
    public Op transform(OpDistinct opDistinct, Op subOp) {
        if(!(subOp instanceof OpProject)){
            return opDistinct;
        }

        OpProject p = (OpProject) subOp;
        return new OpDistinctProject(p.getSubOp(), p.getVars());
    }
}
