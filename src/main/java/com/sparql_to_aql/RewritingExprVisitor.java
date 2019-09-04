package com.sparql_to_aql;

import com.aql.algebra.expressions.constants.Const_Bool;
import com.aql.algebra.expressions.constants.Const_Number;
import com.aql.algebra.expressions.constants.Const_String;
import com.aql.algebra.expressions.functions.Expr_Equals;
import com.aql.algebra.expressions.functions.Expr_LogicalNot;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.*;

import java.util.LinkedList;
import java.util.Map;

//TODO IMP consider removing data type for literals from our arangodb documents - you'll know the type if the value is a number, a string, or a boolean, and if it's a language string it will have lang attribute

//TODO it's gonna be hard to support expressions outside of filters ie. where we need the result of an expression.. possibly mention this in limitations..
// because we'll need to apply calculations on the VALUE of the arango doc but then we need the TYPE, DATATYPE, LANG, as well..
// ideally we know the op in which the expr is being used.. so we can add some other conditions
//Expr is just an interface and there are other classes that implement it and represent operators - refer to https://jena.apache.org/documentation/javadoc/arq/org/apache/jena/sparql/expr/Expr.html
public class RewritingExprVisitor extends ExprVisitorBase {
    private String aqlExpressionString;

    Map<String, String> boundVariables;

    private LinkedList<com.aql.algebra.expressions.Expr> createdAqlExprs = new LinkedList<>();

    public com.aql.algebra.expressions.Expr getFinalAqlExpr(){
        return createdAqlExprs.getFirst();
    }

    public RewritingExprVisitor(Map<String, String> boundVariables){
        this.boundVariables = boundVariables;
    }

    //handle function with no arguments
    @Override
    public void visit(ExprFunction0 func){
        //only expr we might support is E_Now here - this needs to be transformed to DATE_ISO8601(DATE_NOW()) in AQL
        throw new UnsupportedOperationException("SPARQL expression not supported!");
    }

    //handle function with one argument
    @Override
    public void visit(ExprFunction1 func){
        //not a priority but possibly support ABS, CEILING, FLOOR, ROUND, STR_LOWER, STR_UPPER, STR_LENGTH
        // remember that for this we'd have to check the VALUE of the arangodb document
        if(func instanceof E_LogicalNot){
            //TODO
        }
        else{
            throw new UnsupportedOperationException("SPARQL expression not supported!");
        }
    }

    //TODO implement below + consider improving using enum + switch
    @Override
    public void visit(ExprFunction2 func){
        if(func instanceof E_Add){

        }else if(func instanceof E_Divide){

        }else if(func instanceof E_Equals){

        }else if(func instanceof E_GreaterThan){

        }else if(func instanceof E_GreaterThanOrEqual){

        }else if(func instanceof E_LessThan){

        }else if(func instanceof E_LessThanOrEqual){

        }else if(func instanceof E_LogicalAnd){

        }else if(func instanceof E_LogicalOr){

        }else if(func instanceof E_Multiply){

        }else if(func instanceof E_NotEquals){

        }else if(func instanceof E_Subtract){

        }
        else{
            throw new UnsupportedOperationException("SPARQL expression not supported!");
        }
    }

    //encountered in case like (x == y) ? opt1 : opt2
    @Override
    public void visit(ExprFunction3 func){
        throw new UnsupportedOperationException("SPARQL expression not supported!");
    }

    @Override
    public void visit(ExprFunctionN func){
        throw new UnsupportedOperationException("SPARQL expression not supported!");
    }

    //handle function that executes over a graph pattern (E_Exists, E_NotExists)
    @Override
    public void visit(ExprFunctionOp op){
        throw new UnsupportedOperationException("SPARQL expression not supported!");
    }

    @Override
    public void visit(NodeValue nv){
        //handle different types of values - we're only gonna allow "constants"
        if(nv.isBoolean()){
            new Const_Bool(nv.getBoolean());
        }
        else if(nv.isString()) {
            new Const_String(nv.getString());
        }
        else if(nv.isNumber()){
            new Const_Number(nv.getDouble());
        }
        else{
            throw new UnsupportedOperationException("Node value in SPARQL expression not supported!");
        }
    }

    @Override
    public void visit(ExprVar nv){
        //TODO
        nv.getVarName();
    }

    @Override
    public void visit(ExprAggregator eAgg){
        throw new UnsupportedOperationException("SPARQL expression not supported!");
    }

    @Override
    public void visit(ExprNone exprNone){
        throw new UnsupportedOperationException("SPARQL expression not supported!");
    }
}
