package com.sparql_to_aql;

import org.apache.jena.sparql.algebra.op.OpDistinct;

//class used for initial traversal and optimization of SPARQL algebra expression
public class SparqlOptimizationVisitor extends RewritingOpVisitorBase {

    @Override
    public void visit(OpDistinct opDistinct){
        //TODO distinct in AQL is done either using RETURN DISTINCT or using COLLECT..
        //combine this with project using custom OpProjectDistinct or replace it with an OpCollect somehow
    }
}

