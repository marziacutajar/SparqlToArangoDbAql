package com.sparql_to_aql;

import com.sparql_to_aql.entities.algebra.OpDistinctProject;
import org.apache.jena.sparql.algebra.op.OpDistinct;
import org.apache.jena.sparql.algebra.op.OpProject;

//class used for initial traversal and optimization of SPARQL algebra expression
public class SparqlOptimizationVisitor extends RewritingOpVisitorBase {

    @Override
    public void visit(OpProject opProject){
        if(opProject.getVars().size() == 1){
            //TODO I think I'd need to use a custom Transformer here as the below won't work...
            //opProject = new OpDistinctProject(opProject.getSubOp(), opProject.getVars());
        }
    }

    @Override
    public void visit(OpDistinct opDistinct){
        //TODO distinct in AQL is done either using RETURN DISTINCT or using COLLECT..
        //combine this with project using custom OpProjectDistinct or replace it with an OpCollect somehow
    }
}

