package com.sparql_to_aql.utils;

import com.aql.algebra.expressions.constants.Const_Bool;
import com.aql.algebra.expressions.constants.Const_String;
import com.aql.algebra.expressions.functions.Expr_Equals;
import com.aql.algebra.expressions.functions.Expr_LogicalAnd;
import com.aql.algebra.expressions.functions.Expr_LogicalOr;
import com.aql.algebra.expressions.functions.Expr_ToString;
import com.sparql_to_aql.RewritingExprVisitor;
import com.sparql_to_aql.constants.ArangoAttributes;
import com.sparql_to_aql.constants.NodeRole;
import com.sparql_to_aql.constants.RdfObjectTypes;
import com.sparql_to_aql.constants.TransformationModel;
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

        String currAqlVarName = AqlUtils.buildVar(forLoopVarName, attributeName);

        ProcessTripleNode(node, currAqlVarName, filterConditions, boundVars, role.equals(NodeRole.PREDICATE));
    }

    public static void ProcessTripleNode(Node node, String currAqlVarName, com.aql.algebra.expressions.ExprList filterConditions, Map<String, String> boundVars, boolean isPredicate){
        //We can have a mixture of LET and FILTER statements after each other - refer to https://www.arangodb.com/docs/stable/aql/operations-filter.html
        //IMP: in ARQ query expression, blank nodes are represented as variables ??0, ??1 etc.. and an Invalid SPARQL query error is given if same blank node is used in more than one subquery
        if(node.isVariable()) {
            String var_name = node.getName();
            if(boundVars.containsKey(var_name)){
                //node was already bound in another triple, add a filter condition instead
                filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(currAqlVarName), com.aql.algebra.expressions.Var.alloc(boundVars.get(var_name))));
            }
            else {
                //add variable to list of already used/bound vars
                boundVars.put(var_name, currAqlVarName);
            }
        }
        else if(node.isURI()){
            if(!isPredicate)
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

        //cast ArangoAttributes.VALUE to string - another option would be to handle different literal data types ie. cast literal value (literal.getLiteralValue()) according to type instead
        filterConditions.add(new Expr_Equals(new Expr_ToString(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(currAqlVarName, ArangoAttributes.VALUE))), new Const_String(literal.getLiteralLexicalForm())));
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
        String prefix = newLetOrForLoopVarName == null ? "" : newLetOrForLoopVarName + ".";
        for (String sparqlVar : boundVariables.keySet()){
            boundVariables.put(sparqlVar, prefix + sparqlVar);
        }

        return boundVariables;
    }

    public static com.aql.algebra.expressions.VarExprList CreateCollectVarExprList(List<Var> varsForExprList, Map<String, String> boundVars){
        com.aql.algebra.expressions.VarExprList varExprList = new com.aql.algebra.expressions.VarExprList();

        for(Var v: varsForExprList){
            com.aql.algebra.expressions.Var aqlProjectVar = com.aql.algebra.expressions.Var.alloc(v.getVarName());
            com.aql.algebra.expressions.Expr varExpr = new Expr_Equals(aqlProjectVar, com.aql.algebra.expressions.Var.alloc(boundVars.get(v.getVarName())));
            varExprList.add(aqlProjectVar, varExpr);
        }

        return varExprList;
    }

    public static com.aql.algebra.expressions.VarExprList CreateProjectionVarExprList(List<Var> varsForExprList, Map<String, String> boundVars){
        com.aql.algebra.expressions.VarExprList varExprList = new com.aql.algebra.expressions.VarExprList();

        for(Var v: varsForExprList){
            com.aql.algebra.expressions.Var aqlProjectVar = com.aql.algebra.expressions.Var.alloc(v.getVarName());
            com.aql.algebra.expressions.Expr varExpr = com.aql.algebra.expressions.Var.alloc(boundVars.get(v.getVarName()));
            varExprList.add(aqlProjectVar, varExpr);
        }

        return varExprList;
    }

    public static com.aql.algebra.expressions.VarExprList CreateProjectionVarExprList(Map<String, String> boundVars){
        com.aql.algebra.expressions.VarExprList varExprList = new com.aql.algebra.expressions.VarExprList();

        for(String v: boundVars.keySet()){
            com.aql.algebra.expressions.Var aqlProjectVar = com.aql.algebra.expressions.Var.alloc(v);
            com.aql.algebra.expressions.Expr varExpr = com.aql.algebra.expressions.Var.alloc(boundVars.get(v));
            varExprList.add(aqlProjectVar, varExpr);
        }

        return varExprList;
    }

    public static com.aql.algebra.expressions.ExprList GetFiltersOnCommonVars(Map<String, String> leftBoundVars, Map<String, String> rightBoundVars){
        Set<String> commonVars = MapUtils.GetCommonMapKeys(leftBoundVars, rightBoundVars);
        com.aql.algebra.expressions.ExprList filtersExprs = new com.aql.algebra.expressions.ExprList();
        for (String commonVar: commonVars){
            filtersExprs.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(leftBoundVars.get(commonVar))), com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(rightBoundVars.get(commonVar)))));
        }

        return filtersExprs;
    }

}
