package com.sparql_to_aql;

import com.aql.algebra.expressions.constants.Const_Bool;
import com.aql.algebra.expressions.constants.Const_Null;
import com.aql.algebra.expressions.constants.Const_Number;
import com.aql.algebra.expressions.constants.Const_String;
import com.aql.algebra.expressions.functions.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.*;
import org.apache.jena.sparql.expr.ExprFunction0;
import org.apache.jena.sparql.expr.ExprFunction1;
import org.apache.jena.sparql.expr.ExprFunction2;
import org.apache.jena.sparql.expr.ExprFunction3;
import org.apache.jena.sparql.expr.ExprFunctionN;
import java.util.LinkedList;
import java.util.List;
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

    private List<com.aql.algebra.expressions.Expr> GetCreatedAqlExprsForExpr(int numOfNestedExprs){
        int from = createdAqlExprs.size() - numOfNestedExprs;
        int to = createdAqlExprs.size() - 1;
        List<com.aql.algebra.expressions.Expr> exprsToReturn = createdAqlExprs.subList(from, to);
        createdAqlExprs.removeAll(exprsToReturn);
        return exprsToReturn;
    }

    public com.aql.algebra.expressions.Expr getFinalAqlExpr(){
        return createdAqlExprs.get(0);
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
            createdAqlExprs.push(new Expr_LogicalNot(createdAqlExprs.removeLast()));
        }
        else if(func instanceof E_Bound) {
            createdAqlExprs.push(new Expr_NotEquals(createdAqlExprs.removeLast(), new Const_Null()));
        }else{
            throw new UnsupportedOperationException("SPARQL expression not supported!");
        }
    }

    //TODO consider improving using enum + switch
    @Override
    public void visit(ExprFunction2 func){
        com.aql.algebra.expressions.Expr param1 = createdAqlExprs.removeLast();
        com.aql.algebra.expressions.Expr param2 = createdAqlExprs.removeLast();

        com.aql.algebra.expressions.Expr aqlExpr;

        if(func instanceof E_Add){
            aqlExpr = new Expr_Add(param1, param2);
        }else if(func instanceof E_Divide){
            aqlExpr = new Expr_Divide(param1, param2);
        }else if(func instanceof E_Equals){
            aqlExpr = new Expr_Equals(param1, param2);
        }else if(func instanceof E_GreaterThan){
            aqlExpr = new Expr_GreaterThan(param1, param2);
        }else if(func instanceof E_GreaterThanOrEqual){
            aqlExpr = new Expr_GreaterThanOrEqual(param1, param2);
        }else if(func instanceof E_LessThan){
            aqlExpr = new Expr_LessThan(param1, param2);
        }else if(func instanceof E_LessThanOrEqual){
            aqlExpr = new Expr_LessThanOrEqual(param1, param2);
        }else if(func instanceof E_LogicalAnd){
            aqlExpr = new Expr_LogicalAnd(param1, param2);
        }else if(func instanceof E_LogicalOr){
            aqlExpr = new Expr_LogicalOr(param1, param2);
        }else if(func instanceof E_Multiply){
            aqlExpr = new Expr_Multiply(param1, param2);
        }else if(func instanceof E_NotEquals){
            aqlExpr = new Expr_NotEquals(param1, param2);
        }else if(func instanceof E_Subtract){
            aqlExpr = new Expr_Subtract(param1, param2);
        }
        else{
            throw new UnsupportedOperationException("SPARQL expression not supported!");
        }

        createdAqlExprs.push(aqlExpr);
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
        com.aql.algebra.expressions.Expr aqlExpr;

        if(nv.isBoolean()){
            aqlExpr = new Const_Bool(nv.getBoolean());
        }
        else if(nv.isString()) {
            aqlExpr = new Const_String(nv.getString());
        }
        else if(nv.isNumber()){
            aqlExpr = new Const_Number(nv.getDouble());
        }
        else if(nv.isIRI()) {
            aqlExpr = new Const_String(nv.asString());
        }
        else{
            throw new UnsupportedOperationException("Node value in SPARQL expression not supported!");
        }

        createdAqlExprs.push(aqlExpr);
    }

    @Override
    public void visit(ExprVar e){
        createdAqlExprs.push(new com.aql.algebra.expressions.ExprVar(boundVariables.get(e.getVarName())));
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
