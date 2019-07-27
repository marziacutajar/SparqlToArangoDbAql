package com.sparql_to_aql.entities.algebra.transformers;

import com.sparql_to_aql.entities.algebra.OpDistinctProject;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformBase;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpDistinct;
import org.apache.jena.sparql.algebra.op.OpProject;

//IMP: Extending TransformCopy, not TransformBase as the latter doesn't transform a tree
public class OpDistinctTransformer extends TransformCopy {

    @Override
    public Op transform(OpDistinct opDistinct, Op subOp) {
        if(subOp instanceof OpProject){
            OpProject p = (OpProject) subOp;

            //if(p.getVars().size() == 1) {
                return new OpDistinctProject(p.getSubOp(), p.getVars());
            /*}
            else{
                //TODO introduce COLLECT/GROUP operator.. OR I might be able to do without it..
                //if I process OpDistinctProject and if >1 var is projected, add both a COLLECT and RETURN clause after it!
            }*/
        }

        return opDistinct;
    }
}

//op = Transformer.transform(new QueryCleaner(), op); // clean query