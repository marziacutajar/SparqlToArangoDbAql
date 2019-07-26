package com.sparql_to_aql.utils;

import com.sparql_to_aql.constants.NodeRole;
import com.sparql_to_aql.constants.arangodb.AqlOperators;
import org.apache.jena.base.Sys;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.Table;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;

import java.util.Iterator;
import java.util.List;

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
        List<Var> vars = table.getVars();
        for (Iterator<Binding> i = table.rows(); i.hasNext();){
            ProcessBinding(i.next(), vars);
            if(i.hasNext())
                System.out.println(" " + AqlOperators.OR + " ");
        }

    }

    public static void ProcessBinding(Binding binding, List<Var> vars){
        //TODO below consider whether the binding is to a literal or uri..
        System.out.print("(");
        for(Var var : vars){
            System.out.print(var.getVarName() + " " + AqlOperators.EQUALS + " " + binding.get(var).toString());
            if(vars.get(vars.size() -1 ) != var){
                System.out.print(" " + AqlOperators.AND + " ");
            }
        }
        System.out.println(")");
    }


}
