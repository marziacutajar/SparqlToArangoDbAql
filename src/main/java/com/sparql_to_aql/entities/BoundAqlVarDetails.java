package com.sparql_to_aql.entities;

public class BoundAqlVarDetails {
    private String aqlVarName;
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
