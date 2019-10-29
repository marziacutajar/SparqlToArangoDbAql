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

    /**
     * This method is only applicable to our basic approach of RDF storage in ArangoDB
     * Process the subject/predicate/object of a triple pattern, where the AQL variable representing it must be constructed according to the node type
     * @param node the subject, predicate or object in a triple pattern
     * @param role position of the node
     * @param forLoopVarName AQL variable name of the current forloop
     * @param filterConditions list of current forloop filter conditions, to which more conditions can be appended
     * @param boundVars map of all bound SPARQL variables to AQL variables in the current scope
     */
    public static void ProcessTripleNode(Node node, NodeRole role, String forLoopVarName, com.aql.algebra.expressions.ExprList filterConditions, Map<String, String> boundVars){
        String attributeName;

        //since forLoopVarName enumerates some ArangoDB document representing an RDF triple, append the appropriate
        //property name according to the node's role
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

    /**
     * Process the subject/predicate/object of a triple pattern, where the AQL variable representing it is passed to the method
     * @param node the subject, predicate or object in a triple pattern
     * @param currAqlVarName AQL variable name representing the node
     * @param filterConditions list of current forloop filter conditions, to which more conditions can be appended
     * @param boundVars map of all bound SPARQL variables to AQL variables in the current scope
     * @param isPredicate boolean value specifying if the node is in the predicate position in the triple pattern
     */
    public static void ProcessTripleNode(Node node, String currAqlVarName, com.aql.algebra.expressions.ExprList filterConditions, Map<String, String> boundVars, boolean isPredicate){
        //IMP: in ARQ query expression, blank nodes are represented as variables ??0, ??1 etc.. thus no need to check node.isBlank()
        if(node.isVariable()) {
            String var_name = node.getName();
            if(boundVars.containsKey(var_name)){
                //variable was already bound in another triple, thus add a filter condition to make sure this node will match the value already bound to the variable
                filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(currAqlVarName), com.aql.algebra.expressions.Var.alloc(boundVars.get(var_name))));
            }
            else {
                //add variable to list of currently bound variables
                boundVars.put(var_name, currAqlVarName);
            }
        }
        else if(node.isURI()){
            if(!isPredicate){
                //we only apply this if the node is not a predicate, because predicates can only be IRIs anyway - adding this condition would be futile
                filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(currAqlVarName, ArangoAttributes.TYPE)), new Const_String(RdfObjectTypes.IRI)));
            }

            //only match triples having the specified uri in the position represented by the node
            filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(currAqlVarName, ArangoAttributes.VALUE)), new Const_String(node.getURI())));
        }
        else if(node.isLiteral()){
            ProcessLiteralNode(node, currAqlVarName, filterConditions);
        }
    }

    /**
     * Process a literal node in some triple pattern
     * @param literal the node representing the literal
     * @param currAqlVarName AQL variable name representing the node
     * @param filterConditions list of current forloop filter conditions, to which we append more conditions
     */
    private static void ProcessLiteralNode(Node literal, String currAqlVarName, com.aql.algebra.expressions.ExprList filterConditions){
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

    /**
     * Process a SPARQL VALUES clause
     * @param table table containing the data in the VALUES clause
     * @param boundVars map of all bound SPARQL variables to AQL variables in the current scope
     * @return list of filtering expressions
     */
    public static com.aql.algebra.expressions.ExprList ProcessBindingsTableJoin(Table table, Map<String, String> boundVars){
        //the FILTER commands should be added if there is a JOIN clause between a Bindings table and some other op..
        //thus the functionality below could be changed to represent the Table in Arango (ex. array of objects with the bound vars..)
        com.aql.algebra.expressions.Expr jointFilterExpr = null;
        List<Var> vars = table.getVars();

        //process table row by row
        for (Iterator<Binding> i = table.rows(); i.hasNext();){
            com.aql.algebra.expressions.Expr currExpr = ProcessBinding(i.next(), vars, boundVars);

            //append the expr for the current row to the whole expression
            if(jointFilterExpr == null){
                jointFilterExpr = currExpr;
            }
            else{
                //an OR operator is used because the data being joined can match either one of the rows (solutions) in the table
                jointFilterExpr = new Expr_LogicalOr(jointFilterExpr, currExpr);
            }
        }

        return new com.aql.algebra.expressions.ExprList(jointFilterExpr);
    }

    /**
     * Process a single solution/row in a VALUES clause/table
     * @param binding the row being processed
     * @param vars all the SPARQL variables being bound by the solutions in the VALUES clause
     * @param boundVars map of all bound SPARQL variables to AQL variables in the current scope
     * @return
     */
    public static com.aql.algebra.expressions.Expr ProcessBinding(Binding binding, List<Var> vars, Map<String, String> boundVars){
        int undefinedVarsAmount = 0;
        com.aql.algebra.expressions.Expr wholeExpr = null;

        for(Var var : vars){
            Node value = binding.get(var);
            com.aql.algebra.expressions.Expr currExpr;
            if(value == null) {
                //the variable is bound to an undefined value, that is the value of that variable can be anything in this binding case
                //if all values in one binding solution (one row of the table) are all null (UNDEF), then result set shouldn't be filtered
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

            //append the expr for the current variable binding to the whole expression
            if(wholeExpr == null){
                wholeExpr = currExpr;
            }
            else{
                wholeExpr = new Expr_LogicalAnd(wholeExpr, currExpr);
            }
        }

        return wholeExpr;
    }

    /**
     * Process a SPARQL expression - use custom ExprVisitor and get the generated equivalent AQL expression from it
     * @param expr the expression being processed
     * @param boundVariables map of all bound SPARQL variables to AQL variables in the current scope
     * @return AQL expression
     */
    public static com.aql.algebra.expressions.Expr ProcessExpr(Expr expr, Map<String, String> boundVariables){
        RewritingExprVisitor exprVisitor = new RewritingExprVisitor(boundVariables);
        Walker.walk(expr, exprVisitor);

        return exprVisitor.getFinalAqlExpr();
    }

    /**
     * Update the mapping of SPARQL variables to AQL variables, possibly due to the introduction of a new LET or FOR AQL statement
     * @param boundVariables  map of current bindings of SPARQL variables to AQL variables
     * @param newLetOrForLoopVarName the variable name in the new LET or FOR statement that will be used to update the bindings
     * @return
     */
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

    /**
     * Create list of AQL variable expressions for projection (ie. data projected by a SELECT/RETURN statement)
     * @param varsForExprList names of SPARQL variables to be projected
     * @param boundVars map of all bound SPARQL variables to AQL variables in the current scope
     * @return list of projection variable expressions
     */
    public static com.aql.algebra.expressions.VarExprList CreateProjectionVarExprList(List<Var> varsForExprList, Map<String, String> boundVars){
        com.aql.algebra.expressions.VarExprList varExprList = new com.aql.algebra.expressions.VarExprList();

        for(Var v: varsForExprList){
            com.aql.algebra.expressions.Var aqlProjectVar = com.aql.algebra.expressions.Var.alloc(v.getVarName());
            com.aql.algebra.expressions.Expr varExpr = com.aql.algebra.expressions.Var.alloc(boundVars.get(v.getVarName()));
            varExprList.add(aqlProjectVar, varExpr);
        }

        return varExprList;
    }

    /**
     * Create list of AQL variable expressions for projection
     * @param boundVars map of all bound SPARQL variables to AQL variables in the current scope, all of which need to be projected
     * @return list of projection variable expressions
     */
    public static com.aql.algebra.expressions.VarExprList CreateProjectionVarExprList(Map<String, String> boundVars){
        com.aql.algebra.expressions.VarExprList varExprList = new com.aql.algebra.expressions.VarExprList();

        for(String v: boundVars.keySet()){
            com.aql.algebra.expressions.Var aqlProjectVar = com.aql.algebra.expressions.Var.alloc(v);
            com.aql.algebra.expressions.Expr varExpr = com.aql.algebra.expressions.Var.alloc(boundVars.get(v));
            varExprList.add(aqlProjectVar, varExpr);
        }

        return varExprList;
    }

    /**
     * Find common variables between two maps of bound variables, and make sure each common variable has the same value in both maps
     * This is necessary when joining the results of two graph patterns
     * @param leftBoundVars
     * @param rightBoundVars
     * @return
     */
    public static com.aql.algebra.expressions.ExprList GetFiltersOnCommonVars(Map<String, String> leftBoundVars, Map<String, String> rightBoundVars){
        Set<String> commonVars = MapUtils.GetCommonMapKeys(leftBoundVars, rightBoundVars);
        com.aql.algebra.expressions.ExprList filtersExprs = new com.aql.algebra.expressions.ExprList();
        for (String commonVar: commonVars){
            filtersExprs.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(leftBoundVars.get(commonVar))), com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(rightBoundVars.get(commonVar)))));
        }

        return filtersExprs;
    }

}
