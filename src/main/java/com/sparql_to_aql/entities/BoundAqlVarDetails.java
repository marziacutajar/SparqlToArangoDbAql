package com.sparql_to_aql.entities;

public class BoundAqlVarDetails {
    private String aqlVarName;

    public BoundAqlVarDetails(String aqlVarName){
        this.aqlVarName = aqlVarName;
    }

    public BoundAqlVarDetails(String aqlVarName, boolean canBeNull){
        this.aqlVarName = aqlVarName;
    }

    public String getAqlVarName(){
        return aqlVarName;
    }

    public void updateBoundAqlVar(String newAqlVarName){
        this.aqlVarName = newAqlVarName;
    }

}
