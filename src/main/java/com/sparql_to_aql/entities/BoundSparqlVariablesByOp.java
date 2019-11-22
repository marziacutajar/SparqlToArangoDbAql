package com.sparql_to_aql.entities;

import com.sparql_to_aql.utils.MapUtils;

import java.util.HashMap;
import java.util.Map;

public class BoundSparqlVariablesByOp {

    //Keep track of which variables have already been bound (or not, if optional), by mapping ARQ algebra op hashcode to the list of vars
    //the second map is used to map the sparql variable name to the corresponding aql variable name(s) to use (due to for loop variable names)
    private Map<Integer, Map<String, BoundAqlVars>> boundSparqlVariablesByOp = new HashMap<>();

    //keep record of the latest map of boundVariables added to boundSparqlVariablesByOp, for usage in RewritingExprVisitor due to EXISTS and NOT EXISTS graph patterns
    private Map<String, BoundAqlVars> lastBoundVars = new HashMap<>();

    /**
     * Set map of bound SPARQL to AQL variables in the scope of a particular SPARQL operator
     * @param op SPARQL operator
     * @param variables map of bound SPARQL to AQL variables
     */
    public void setSparqlVariablesByOp(org.apache.jena.sparql.algebra.Op op, Map<String, BoundAqlVars> variables){
        boundSparqlVariablesByOp.put(op.hashCode(), variables);

        lastBoundVars = variables;
    }

    public Map<String, BoundAqlVars> getSparqlVariablesByOp(org.apache.jena.sparql.algebra.Op op){
        Map<String, BoundAqlVars> currUsedVars = boundSparqlVariablesByOp.get(op.hashCode());
        if(currUsedVars == null)
            return new HashMap<>();

        return currUsedVars;
    }

    public Map<String, BoundAqlVars> getLastBoundVars(){
        return lastBoundVars;
    }
}
