package com.sparql_to_aql;

import org.apache.jena.sparql.expr.*;

public class RewritingExprVisitor extends ExprVisitorBase {
    private String aqlExpressionString;

    //handle function with no arguments
    @Override
    public void visit(ExprFunction0 func){

    }

    //handle function with one argument
    @Override
    public void visit(ExprFunction1 func){
        //TODO check which function it is
        func.getArg();
        //TODO also visit the arguments ?? or if this is done bottom-up by the visitor then no need..
    }

    @Override
    public void visit(ExprFunction2 func){
        func.getArg1();
        func.getArg2();
    }

    //TODO not sure if I need this... I think it's encountered in case like (x) ? opt1 : opt2
    @Override
    public void visit(ExprFunction3 func){

    }

    //TODO not sure if I'll support this
    @Override
    public void visit(ExprFunctionN func){

    }

    //handle function that executes over a graph pattern (E_Exists, E_NotExists)
    @Override
    public void visit(ExprFunctionOp op){
        //TODO not sure how to handle this in AQL..
    }

    @Override
    public void visit(NodeValue nv){
        //TODO handle different types of values

        if(nv.isIRI()){
            nv.getString();
        }
        //TODO how do we handle a literal here if for literals we need multiple conditions....!
        else if(nv.isLiteral()){
            if(nv.isBoolean()){
                nv.getBoolean();
            }
            else if(nv.isDate()){
                //TODO decide between getDate, asString and asQuotedString
                nv.asString();
            }
            else if(nv.isDateTime()){

            }
            else if(nv.isLangString()){

            }
            else if(nv.isString()) {

            }
            else if(nv.isNumber()){
                //TODO check what type of number...
            }
        }
        else if(nv.isBlank()){

        }
    }

    @Override
    public void visit(ExprVar nv){
        nv.getVarName();
    }

    @Override
    public void visit(ExprAggregator eAgg){
        //TODO not sure how to handle
    }

    @Override
    public void visit(ExprNone exprNone){
        //represents null
        System.out.println("null");
    }
}
