package com.sparql_to_aql.utils;

public class VariableGenerator {
    private int counter;

    private String prefix;
    private String suffix;

    public VariableGenerator(String prefix, String suffix){
        counter = 0;
        this.prefix = prefix;
        this.suffix = suffix;
    }

    public String getCurrent(){
        return prefix + counter + suffix;
    }

    public String getNew(){
        counter++;
        return getCurrent();
    }
}
