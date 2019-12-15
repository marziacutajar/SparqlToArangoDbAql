package com.sparql_to_aql;

import com.aql.algebra.AqlQueryNode;
import com.aql.algebra.expressions.ExprSubquery;
import com.aql.algebra.expressions.constants.*;
import com.aql.algebra.expressions.functions.*;
import com.aql.algebra.operators.OpProject;
import com.aql.algebra.operators.OpSequence;
import com.sparql_to_aql.constants.ArangoAttributes;
import com.sparql_to_aql.constants.ArangoDataModel;
import com.sparql_to_aql.entities.BoundAqlVars;
import com.sparql_to_aql.utils.AqlUtils;
import com.sparql_to_aql.utils.RewritingUtils;
import com.sparql_to_aql.utils.VariableGenerator;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.expr.*;
import org.apache.jena.sparql.expr.ExprFunction0;
import org.apache.jena.sparql.expr.ExprFunction1;
import org.apache.jena.sparql.expr.ExprFunction2;
import org.apache.jena.sparql.expr.ExprFunction3;
import org.apache.jena.sparql.expr.ExprFunctionN;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

//Expr is just an interface and there are other classes that implement it and represent operators - refer to https://jena.apache.org/documentation/javadoc/arq/org/apache/jena/sparql/expr/Expr.html
public class RewritingExprVisitor extends ExprVisitorBase {

    Map<String, BoundAqlVars> boundVariables;
    ArangoDataModel dataModel;

    protected VariableGenerator forLoopVarGenerator;
    protected VariableGenerator assignmentVarGenerator;
    protected VariableGenerator graphForLoopVertexVarGenerator;
    protected VariableGenerator graphForLoopEdgeVarGenerator;
    protected VariableGenerator graphForLoopPathVarGenerator;

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

    public RewritingExprVisitor(Map<String, BoundAqlVars> boundVariables, ArangoDataModel dataModel, VariableGenerator forLoopVarGen, VariableGenerator assignmentVarGen, VariableGenerator graphVertexVarGen, VariableGenerator graphEdgeVarGen, VariableGenerator graphPathVarGen){
        this.boundVariables = boundVariables;
        this.dataModel = dataModel;
        this.forLoopVarGenerator = forLoopVarGen;
        this.assignmentVarGenerator = assignmentVarGen;
        this.graphForLoopEdgeVarGenerator = graphEdgeVarGen;
        this.graphForLoopPathVarGenerator = graphPathVarGen;
        this.graphForLoopVertexVarGenerator = graphVertexVarGen;
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
        if(func instanceof E_LogicalNot){
            createdAqlExprs.add(new Expr_LogicalNot(createdAqlExprs.removeLast()));
        }
        else if(func instanceof E_Bound) {
            createdAqlExprs.add(new Expr_NotEquals(createdAqlExprs.removeLast(), new Const_Null()));
        }
        else if(func instanceof E_Lang){
            createdAqlExprs.add(updateExprVar(createdAqlExprs.removeLast(), ArangoAttributes.LITERAL_LANGUAGE));
        }
        else if (func instanceof E_Str){
            createdAqlExprs.add(new Expr_ToString(updateExprVar(createdAqlExprs.removeLast(), ArangoAttributes.VALUE)));
        }
        else{
            throw new UnsupportedOperationException("SPARQL expression not supported!");
        }
    }

    //TODO consider improving using enum + switch.. or use visitor pattern on them somehow
    @Override
    public void visit(ExprFunction2 func){
        com.aql.algebra.expressions.Expr param2 = createdAqlExprs.removeLast();
        com.aql.algebra.expressions.Expr param1 = createdAqlExprs.removeLast();

        com.aql.algebra.expressions.Expr param2_ExprVarWithValueSubAttr = updateExprVar(param2, ArangoAttributes.VALUE);
        com.aql.algebra.expressions.Expr param1_ExprVarWithValueSubAttr = updateExprVar(param1, ArangoAttributes.VALUE);

        com.aql.algebra.expressions.Expr aqlExpr;

        if(func instanceof E_Add){
            aqlExpr = new Expr_Add(param1_ExprVarWithValueSubAttr, param2_ExprVarWithValueSubAttr);
        }else if(func instanceof E_Divide){
            aqlExpr = new Expr_Divide(param1_ExprVarWithValueSubAttr, param2_ExprVarWithValueSubAttr);
        }else if(func instanceof E_GreaterThan){
            aqlExpr = new Expr_GreaterThan(param1_ExprVarWithValueSubAttr, param2_ExprVarWithValueSubAttr);
        }else if(func instanceof E_GreaterThanOrEqual){
            aqlExpr = new Expr_GreaterThanOrEqual(param1_ExprVarWithValueSubAttr, param2_ExprVarWithValueSubAttr);
        }else if(func instanceof E_LessThan){
            aqlExpr = new Expr_LessThan(param1_ExprVarWithValueSubAttr, param2_ExprVarWithValueSubAttr);
        }else if(func instanceof E_LessThanOrEqual){
            aqlExpr = new Expr_LessThanOrEqual(param1_ExprVarWithValueSubAttr, param2_ExprVarWithValueSubAttr);
        }else if(func instanceof E_Multiply){
            aqlExpr = new Expr_Multiply(param1_ExprVarWithValueSubAttr, param2_ExprVarWithValueSubAttr);
        }else if(func instanceof E_Subtract){
            aqlExpr = new Expr_Subtract(param1_ExprVarWithValueSubAttr, param2_ExprVarWithValueSubAttr);
        }else if(func instanceof E_LogicalAnd){
            aqlExpr = new Expr_LogicalAnd(param1, param2);
        }else if(func instanceof E_LogicalOr){
            aqlExpr = new Expr_LogicalOr(param1, param2);
        }else if(func instanceof E_Equals || func instanceof E_NotEquals){
            //TODO what if one of the params is a constant json object ie. represents a document (happens when have a literal or IRI node value)? then we want to compare the whole document to another document ie. don't change the var name
            //if we are comparing a constant value to a variable, we have to use .VALUE attribute, otherwise not..
            if(param1 instanceof com.aql.algebra.expressions.ExprVar && !(param2 instanceof com.aql.algebra.expressions.ExprVar))
                param1 = new com.aql.algebra.expressions.ExprVar(AqlUtils.buildVar(param1.getVarName(), ArangoAttributes.VALUE));
            else if(param2 instanceof com.aql.algebra.expressions.ExprVar && !(param1 instanceof com.aql.algebra.expressions.ExprVar))
                param2 = new com.aql.algebra.expressions.ExprVar(AqlUtils.buildVar(param2.getVarName(), ArangoAttributes.VALUE));

            //we need to handle things differently for graph approach - we need to add multiple equals filter conds matching the attributes
            if (dataModel == ArangoDataModel.G && param1 instanceof com.aql.algebra.expressions.ExprVar && param2 instanceof com.aql.algebra.expressions.ExprVar){
                aqlExpr = new Expr_Equals(new com.aql.algebra.expressions.ExprVar(AqlUtils.buildVar(param1.getVarName(), ArangoAttributes.TYPE)), new com.aql.algebra.expressions.ExprVar(AqlUtils.buildVar(param2.getVarName(), ArangoAttributes.TYPE)));
                aqlExpr = new Expr_LogicalAnd(aqlExpr, new Expr_Equals(new com.aql.algebra.expressions.ExprVar(AqlUtils.buildVar(param1.getVarName(), ArangoAttributes.VALUE)), new com.aql.algebra.expressions.ExprVar(AqlUtils.buildVar(param2.getVarName(), ArangoAttributes.VALUE))));
                aqlExpr = new Expr_LogicalAnd(aqlExpr, new Expr_Equals(new com.aql.algebra.expressions.ExprVar(AqlUtils.buildVar(param1.getVarName(), ArangoAttributes.LITERAL_DATA_TYPE)), new com.aql.algebra.expressions.ExprVar(AqlUtils.buildVar(param2.getVarName(), ArangoAttributes.LITERAL_DATA_TYPE))));
                aqlExpr = new Expr_LogicalAnd(aqlExpr, new Expr_Equals(new com.aql.algebra.expressions.ExprVar(AqlUtils.buildVar(param1.getVarName(), ArangoAttributes.LITERAL_LANGUAGE)), new com.aql.algebra.expressions.ExprVar(AqlUtils.buildVar(param2.getVarName(), ArangoAttributes.LITERAL_LANGUAGE))));

                if(func instanceof E_NotEquals)
                    aqlExpr = new Expr_LogicalNot(aqlExpr);
            }
            else {
                if(func instanceof E_Equals)
                    aqlExpr = new Expr_Equals(param1, param2);
                else
                    aqlExpr = new Expr_NotEquals(param1, param2);
            }
        }
        else if(func instanceof E_LangMatches){
            if(func.getArg2().getConstant().getString().equals("*")){
                aqlExpr = new Expr_NotEquals(param1, new Const_Null());
            }
            else{
                //consider using Expr_Like function instead of Equals here.. since langMatches in SPARQL would match, for example, "en-GB" even if the second parameter is "EN"
                aqlExpr = new Expr_Equals(new Expr_Lower(param1), new Expr_Lower(param2));
            }
        }
        else{
            throw new UnsupportedOperationException("SPARQL expression not supported!");
        }

        createdAqlExprs.add(aqlExpr);
    }

    //encountered in case like (x == y) ? opt1 : opt2
    @Override
    public void visit(ExprFunction3 func){
        throw new UnsupportedOperationException("SPARQL expression not supported!");
    }

    @Override
    public void visit(ExprFunctionN func){
        int numArgs = func.numArgs();
        int pos = createdAqlExprs.size()-numArgs;
        com.aql.algebra.expressions.ExprList aqlExprs = new com.aql.algebra.expressions.ExprList();
        if(func instanceof E_StrConcat){
            for(int i=0; i < numArgs; i++){
                com.aql.algebra.expressions.Expr currParam = createdAqlExprs.remove(pos);
                aqlExprs.add(updateExprVar(currParam, ArangoAttributes.VALUE));
            }
            createdAqlExprs.add(new Expr_Concat(aqlExprs));
        }
        else
            throw new UnsupportedOperationException("SPARQL expression not supported!");
    }

    //handle function that executes over a graph pattern (E_Exists, E_NotExists)
    @Override
    public void visit(ExprFunctionOp func){
        //TODO cater for filter in (not) exists query.. we need to pass the boundVariables map to the new visitor..
        //we need to use an instance of ArqToAqlAlgebra visitor to translate the graph pattern within the FILTER clause
        ArqToAqlAlgebraVisitor subQueryTranslator;
        switch (dataModel){
            case D:
                subQueryTranslator = new ArqToAqlAlgebraVisitor_BasicApproach(null, null, forLoopVarGenerator, assignmentVarGenerator, graphForLoopVertexVarGenerator, graphForLoopEdgeVarGenerator, graphForLoopPathVarGenerator);
                break;
            case G:
                subQueryTranslator = new ArqToAqlAlgebraVisitor_GraphApproach(null, null, forLoopVarGenerator, assignmentVarGenerator, graphForLoopVertexVarGenerator, graphForLoopEdgeVarGenerator, graphForLoopPathVarGenerator);
                break;
            default: throw new UnsupportedOperationException("Unsupported data model!");
        }

        //walk the SPARQL algebra tree of the nested graph pattern using our visitor class, translating it to AQL
        OpWalker.walk(func.getGraphPattern(), subQueryTranslator);

        //get the generated AQL query structure
        OpSequence aqlQueryExpression = subQueryTranslator.GetAqlAlgebraQueryExpression();

        AqlQueryNode finalSubQuery = aqlQueryExpression.getElements().remove(aqlQueryExpression.size() - 1);
        Map<String, BoundAqlVars> subQueryBoundVars = subQueryTranslator.GetLastBoundVars();

        //add join filter statements over finalSubQuery to match the variables bound outside of the FILTER clause
        com.aql.algebra.expressions.ExprList filterExprs = RewritingUtils.GetFiltersOnCommonVars(boundVariables, subQueryBoundVars, dataModel);
        if(filterExprs.size() > 0) {
            finalSubQuery = new com.aql.algebra.operators.OpFilter(filterExprs, finalSubQuery);
        }

        //It is enough for us to project an empty object below.. we're not binding any variables and we just need to count the objects returned in the end
        aqlQueryExpression.add(new OpProject(finalSubQuery, new Const_Object(), false));

        //FILTER LENGTH((NESTED FOR LOOP HERE WITH PROJECT) > 0) if exists (or == 0 if not exists)
        if(func instanceof E_Exists)
            createdAqlExprs.add(new Expr_GreaterThan(new Expr_Length(new ExprSubquery(aqlQueryExpression)), new Const_Number(0)));
        else
            createdAqlExprs.add(new Expr_Equals(new Expr_Length(new ExprSubquery(aqlQueryExpression)), new Const_Number(0)));
    }

    @Override
    public void visit(NodeValue nv){
        //handle different types of values - we're only gonna allow "constants"
        com.aql.algebra.expressions.Expr aqlExpr;

        if(nv.isBoolean()){
            aqlExpr = new Const_Bool(nv.getBoolean());
        }
        /*else if(nv.isLiteral()){
            //TODO we should transform it into an object with type and value here
            aqlExpr = new Const_String(nv.getString());
        }*/
        else if(nv.isIRI()) {
            //TODO we should transform it into an object with type and value here
            aqlExpr = new Const_String(nv.asString());
        }
        else if(nv.isString()) {
            aqlExpr = new Const_String(nv.getString());
        }
        else if(nv.isNumber()){
            aqlExpr = new Const_Number(nv.getDouble());
        }
        else{
            throw new UnsupportedOperationException("Node value in SPARQL expression not supported!");
        }

        createdAqlExprs.add(aqlExpr);
    }

    @Override
    public void visit(ExprVar e){
        createdAqlExprs.add(boundVariables.get(e.getVarName()).asExpr());
    }

    @Override
    public void visit(ExprAggregator eAgg){
        throw new UnsupportedOperationException("SPARQL expression not supported!");
    }

    @Override
    public void visit(ExprNone exprNone){
        throw new UnsupportedOperationException("SPARQL expression not supported!");
    }

    private com.aql.algebra.expressions.Expr updateExprVar(com.aql.algebra.expressions.Expr expr, String subAttribute){
        if(expr instanceof com.aql.algebra.expressions.ExprVar)
            return new com.aql.algebra.expressions.ExprVar(AqlUtils.buildVar(expr.getVarName(), subAttribute));

        return expr;
    }
}
