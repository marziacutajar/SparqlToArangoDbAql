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
        //call MINUS function on left and right sides, assign to a variable using LET
        //consider: what if there are variables on the righthand side not on the left side.. how to handle this? .. I think you just ignore them..
        //if there are no shared variables, do nothing (no matching bindings)
        //consider: what if we use a JOIN with not equals filter conditions to simulate OpMinus, instead of using MINUS function??
        //IMPORTANT: In MINUS operator we only consider variable bindings ex. if we have MINUS(<http://a> <http://b> <http://c>) and this triple is
        //in the results of the lefthand side, that triple won't be removed because there are no bindings
        //there must be at least one common variable between left and right side
        //System.out.println("LET minus_results = MINUS(left_side_results_array, right_side_results_array)");
    }

}
