package com.sparql_to_aql.entities;

public class BoundAqlVarDetails {
    private String aqlVarName;
    // consider renaming canBeNull to something else.. reason being that null and undefined/unbound are not the same in SPARQL
    // when computing the query.. if a variable is bound to a null value and we join it in SPARQL, it's not treated as an optional variable and an equality condition must be satisfied
    // what if we rename it to canBeUnbound?? that way we know that obviously when a projection occurs, the variable HAS to be bound (even if to a null value)
    private boolean canBeNull;

    public BoundAqlVarDetails(String aqlVarName){
        this.aqlVarName = aqlVarName;
        //by default, variables can't be null
        this.canBeNull = false;
    }

    public BoundAqlVarDetails(String aqlVarName, boolean canBeNull){
        this.aqlVarName = aqlVarName;
        this.canBeNull = canBeNull;
    }

    public String getAqlVarName(){
        return aqlVarName;
    }

    public boolean canBeNull(){
        return canBeNull;
    }

    public void updateBoundAqlVar(String newAqlVarName){
        this.aqlVarName = newAqlVarName;
    }

    public void updateCanBeNull(boolean canBeNull){
        this.canBeNull = canBeNull;
    }
}
