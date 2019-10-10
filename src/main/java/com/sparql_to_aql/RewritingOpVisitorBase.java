package com.sparql_to_aql;

import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.*;

public abstract class RewritingOpVisitorBase extends OpVisitorBase {
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
        throw new UnsupportedOperationException("Encountered unsupported operation!");
    }

    @Override
    public void visit(OpGroup opGroup){
        throw new UnsupportedOperationException("The SPARQL GROUPBY operation is not supported!");
    }

    @Override
    public void visit(OpMinus opMinus){
        throw new UnsupportedOperationException("The SPARQL MINUS operation is not supported!");
    }

}
