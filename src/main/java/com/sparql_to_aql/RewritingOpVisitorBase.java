package com.sparql_to_aql;

import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.OpProcedure;
import org.apache.jena.sparql.algebra.op.OpReduced;
import org.apache.jena.sparql.algebra.op.OpService;

public class RewritingOpVisitorBase extends OpVisitorBase {
    @Override
    public void visit(OpReduced opReduced){
        throw new UnsupportedOperationException("The SPARQL REDUCED keyword is not supported!");
    }

    @Override
    public void visit(OpService opService){
        throw new UnsupportedOperationException("The SPARQL SERVICE keyword is not supported!");
    }

    @Override
    public void visit(OpProcedure opProcedure){
        //TODO not sure what the use of this is
        throw new UnsupportedOperationException("Encountered unsupported operation!");
    }
}
