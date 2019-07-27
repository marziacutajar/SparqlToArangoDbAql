package com.sparql_to_aql.utils;

import com.sparql_to_aql.constants.NodeRole;
import com.sparql_to_aql.constants.arangodb.AqlOperators;
import org.apache.jena.base.Sys;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.Table;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprFunction;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class RewritingUtils {
    public static void ProcessTripleNode(Node node, NodeRole role, String forLoopVarName, List<String> usedVars, List<String> filterConditions){
        if(node.isVariable())
            usedVars.add(node.getName());

        //TODO deal with blank nodes
        switch(role){
            case SUBJECT:
                if(node.isVariable()) {
                    System.out.println("LET " + node.getName() + " = " + forLoopVarName +".s");
                }
                else if(node.isURI()){
                    String filterCond = forLoopVarName + ".s " + AqlOperators.EQUALS + " '" + node.getURI() + "'";
                    filterConditions.add(filterCond);
                }
                break;
            case PREDICATE:
                if(node.isVariable()) {
                    System.out.println("LET " + node.getName() + " = " + forLoopVarName + ".p");
                }
                else if(node.isURI()){
                    String filterCond = forLoopVarName + ".p " + AqlOperators.EQUALS + " '" + node.getURI() + "'";
                    filterConditions.add(filterCond);
                    System.out.println(filterCond);
                }
                break;
            case OBJECT:
                if(node.isVariable()) {
                    System.out.println("LET " + node.getName() + " = " + forLoopVarName +".o");
                }
                else if(node.isURI()){
                    String filterCond = forLoopVarName + ".o " + AqlOperators.EQUALS + " '" + node.getURI() + "'";
                    filterConditions.add(filterCond);
                }
                else if(node.isLiteral()){
                    //TODO parse and use type, language etc
                    node.getLiteralDatatypeURI();
                    node.getLiteralLanguage();
                    node.getLiteralLexicalForm();
                }
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    public static void ProcessBindingsTable(Table table){
        //TODO the FILTER commands should be added if there is a JOIN clause between a Bindings table and some other op..
        //thus the functionality below should be changed to represent the Table in Arango (ex. array of objects with the bound vars..)
        String bindingsInAql = "LET bindings = [";
        List<Var> vars = table.getVars();
        for (Iterator<Binding> i = table.rows(); i.hasNext();){
            bindingsInAql += ProcessBinding(i.next(), vars);
            if(i.hasNext()){
                bindingsInAql += ",";
                //System.out.println(" " + AqlOperators.OR + " ");
            }
        }
        bindingsInAql += "]";
        System.out.println(bindingsInAql);
    }

    public static String ProcessBinding(Binding binding, List<Var> vars){
        //TODO below consider whether the binding is to a literal, uri, or UNDEF.. if literal what data type it has, etc...
        // a variable can be bound to an undefined value (ie. it can be any value..)
        String jsonObject = "{";
        //System.out.print("(");
        for(Var var : vars){
            jsonObject += "\"" + var.getVarName() + "\" : " + binding.get(var).toString();
            //System.out.print(var.getVarName() + " " + AqlOperators.EQUALS + " " + binding.get(var).toString());
            if(vars.get(vars.size() -1 ) != var){
                //System.out.print(" " + AqlOperators.AND + " ");
                jsonObject += ",";
            }
        }
        jsonObject += "}";
        return jsonObject;
        //System.out.println(")");
    }

    public static String ProcessExpr(Expr expr){
        String aqlExpr = "";
        //TODO not sure how to evaluate expression.... return the expression in string form and in the form executable in ArangoDB
        ExprFunction expfun = expr.getFunction();
        //get operator TODO translate it to equivalent AQL operator.. need a switch here or something.. or a Map mapping ARQ ops to AQL ops
        expfun.getOpName();
        //TODO arguments can also be expressions... must loop and process them recursively
        expfun.getArgs();

        return expr.toString();
    }

}
