package com.sparql_to_aql.utils;

import com.aql.algebra.expressions.constants.Const_Bool;
import com.aql.algebra.expressions.constants.Const_String;
import com.aql.algebra.expressions.functions.Expr_Equals;
import com.aql.algebra.expressions.functions.Expr_LogicalAnd;
import com.aql.algebra.expressions.functions.Expr_LogicalOr;
import com.sparql_to_aql.RewritingExprVisitor;
import com.sparql_to_aql.constants.ArangoAttributes;
import com.sparql_to_aql.constants.NodeRole;
import com.sparql_to_aql.constants.RdfObjectTypes;
import com.sparql_to_aql.constants.arangodb.AqlOperators;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.impl.RDFLangString;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.Table;
import org.apache.jena.sparql.algebra.walker.Walker;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.*;

import java.util.*;

public class RewritingUtils {
    public static void ProcessTripleNode(Node node, NodeRole role, String forLoopVarName, com.aql.algebra.expressions.ExprList filterConditions, Map<String, String> boundVars){
        String attributeName;

        Set<String> usedSparqlVars = boundVars.keySet();

        switch(role){
            case SUBJECT:
                attributeName = ArangoAttributes.SUBJECT;
                break;
            case PREDICATE:
                attributeName = ArangoAttributes.PREDICATE;
                break;
            case OBJECT:
                attributeName = ArangoAttributes.OBJECT;
                break;
            default:
                throw new UnsupportedOperationException();
        }

        //We can have a mixture of LET and FILTER statements after each other - refer to https://www.arangodb.com/docs/stable/aql/operations-filter.html
        //IMP: in ARQ query expression, blank nodes are represented as variables ??0, ??1 etc.. and an Invalid SPARQL query error is given if same blank node is used in more than one subquery
        String currAqlVarName = AqlUtils.buildVar(forLoopVarName, attributeName);

        if(node.isVariable()) {
            String var_name = node.getName();
            if(usedSparqlVars.contains(var_name)){
                //node was already bound in another triple, add a filter condition instead
                filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(currAqlVarName), com.aql.algebra.expressions.Var.alloc(boundVars.get(var_name))));
            }
            else {
                //add variable to list of already used/bound vars
                boundVars.put(var_name, currAqlVarName);
            }
        }
        else if(node.isURI()){
            filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(currAqlVarName, ArangoAttributes.TYPE)), new Const_String(RdfObjectTypes.IRI)));
            filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(currAqlVarName, ArangoAttributes.VALUE)), new Const_String(node.getURI())));
        }
        else if(node.isLiteral()){
            ProcessLiteralNode(node, currAqlVarName, filterConditions);
        }
    }

    public static void ProcessLiteralNode(Node literal, String currAqlVarName, com.aql.algebra.expressions.ExprList filterConditions){
        //important to compare to data type in Arango object here
        filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(currAqlVarName, ArangoAttributes.TYPE)), new Const_String(RdfObjectTypes.LITERAL)));

        RDFDatatype datatype = literal.getLiteralDatatype();
        filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(currAqlVarName, ArangoAttributes.LITERAL_DATA_TYPE)), new Const_String(datatype.getURI())));

        if (datatype instanceof RDFLangString) {
            filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(currAqlVarName, ArangoAttributes.LITERAL_LANGUAGE)), new Const_String(literal.getLiteralLanguage())));
        }

        //deiced which of these 2 below methods to call to get the value - refer to https://www.w3.org/TR/sparql11-query/#matchingRDFLiterals
        //would probably be easier to use the lexical form everywhere.. that way I don't have to parse by type.. although when showing results to user we'll have to customize their displays according to the type..
        //literal.getLiteralValue();
        //TODO using the lexical form won't work when we want to apply math or string functions to values in AQL!
        filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(currAqlVarName, ArangoAttributes.VALUE)), new Const_String(literal.getLiteralLexicalForm())));
    }

    public static com.aql.algebra.expressions.ExprList ProcessBindingsTableJoin(Table table, Map<String, String> boundVars){
        //the FILTER commands should be added if there is a JOIN clause between a Bindings table and some other op..
        //thus the functionality below could be changed to represent the Table in Arango (ex. array of objects with the bound vars..)
        com.aql.algebra.expressions.Expr jointFilterExpr = null;
        List<Var> vars = table.getVars();
        for (Iterator<Binding> i = table.rows(); i.hasNext();){
            com.aql.algebra.expressions.Expr currExpr = ProcessBinding(i.next(), vars, boundVars);

            if(jointFilterExpr == null){
                jointFilterExpr = currExpr;
            }
            else{
                jointFilterExpr = new Expr_LogicalOr(jointFilterExpr, currExpr);
            }
        }
        return new com.aql.algebra.expressions.ExprList(jointFilterExpr);
    }

    public static com.aql.algebra.expressions.Expr ProcessBinding(Binding binding, List<Var> vars, Map<String, String> boundVars){
        int undefinedVarsAmount = 0;
        com.aql.algebra.expressions.Expr wholeExpr = null;
        for(Var var : vars){
            Node value = binding.get(var);
            com.aql.algebra.expressions.Expr currExpr;
            if(value == null) {
                //the variable is bound to an undefined value, that is the value of that variable can be anything in this binding case
                //if all values in one binding (one row of the table) are all null (UNDEF), then result set shouldn't be filtered
                undefinedVarsAmount++;
                if(undefinedVarsAmount == vars.size()) {
                    currExpr = new Const_Bool(true);
                }
                else{
                    continue;
                }
            }
            else {
                //TODO consider whether the binding is to a literal, or uri.. if literal what data type it has, etc...
                currExpr = new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(boundVars.get(var.getName()))), new Const_String(value.toString()));
            }

            if(wholeExpr == null){
                wholeExpr = currExpr;
            }
            else{
                wholeExpr = new Expr_LogicalAnd(wholeExpr, currExpr);
            }
        }

        return wholeExpr;
    }

    //use custom ExprVisitor and get the generated AQL equvialent expression from it
    public static com.aql.algebra.expressions.Expr ProcessExpr(Expr expr, Map<String, String> boundVariables){
        RewritingExprVisitor exprVisitor = new RewritingExprVisitor(boundVariables);
        Walker.walk(expr, exprVisitor);

        return exprVisitor.getFinalAqlExpr();
    }

    public static Map<String, String> UpdateBoundVariablesMapping(Map<String, String> boundVariables, String newLetOrForLoopVarName){
        for (String sparqlVar : boundVariables.keySet()){
            boundVariables.put(sparqlVar, newLetOrForLoopVarName + "." + sparqlVar);
        }

        return boundVariables;
    }

    public static com.aql.algebra.expressions.VarExprList CreateVarExprList(List<Var> varsForExprList, Map<String, String> boundVars){
        com.aql.algebra.expressions.VarExprList varExprList = new com.aql.algebra.expressions.VarExprList();

        for(Var v: varsForExprList){
            com.aql.algebra.expressions.Var aqlProjectVar = com.aql.algebra.expressions.Var.alloc(v.getVarName());
            com.aql.algebra.expressions.Expr varExpr = new Expr_Equals(aqlProjectVar, com.aql.algebra.expressions.Var.alloc(boundVars.get(v.getVarName())));
            varExprList.add(aqlProjectVar, varExpr);
        }

        return varExprList;
    }

    public static com.aql.algebra.expressions.VarExprList CreateVarExprList(Map<String, String> boundVars){
        com.aql.algebra.expressions.VarExprList varExprList = new com.aql.algebra.expressions.VarExprList();

        for(String v: boundVars.keySet()){
            com.aql.algebra.expressions.Var aqlProjectVar = com.aql.algebra.expressions.Var.alloc(v);
            com.aql.algebra.expressions.Expr varExpr = new Expr_Equals(aqlProjectVar, com.aql.algebra.expressions.Var.alloc(boundVars.get(v)));
            varExprList.add(aqlProjectVar, varExpr);
        }

        return varExprList;
    }

}
