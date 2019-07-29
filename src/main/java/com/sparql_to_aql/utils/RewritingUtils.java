package com.sparql_to_aql.utils;

import com.sparql_to_aql.constants.NodeRole;
import com.sparql_to_aql.constants.arangodb.AqlOperators;
import org.apache.jena.base.Sys;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.impl.RDFLangString;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.Table;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.*;
import org.apache.jena.sparql.util.ExprUtils;

import java.util.Iterator;
import java.util.List;

public class RewritingUtils {
    public static void ProcessTripleNode(Node node, NodeRole role, String forLoopVarName, List<String> usedVars, List<String> filterConditions){
        if(node.isVariable())
            usedVars.add(node.getName());

        String attributeName;

        switch(role){
            case SUBJECT:
                attributeName = "s";
                break;
            case PREDICATE:
                attributeName = "p";
                break;
            case OBJECT:
                attributeName = "o";
                break;
            default:
                throw new UnsupportedOperationException();
        }

        //TODO I think ideally we do the assigning of variables (LET) at the end.. first add the filters then maybe replace assignments with a RETURN clause holding the mapping
        //DEPENDS IF YOU CAN HAVE MIX OF LET AND FILTER AFTER EACH OTHER IN THE SAME FOR LOOP - I think so.. refer to https://www.arangodb.com/docs/stable/aql/operations-filter.html
        if(node.isVariable()) {
            System.out.println("LET " + node.getName() + " = " + forLoopVarName +"." + attributeName);
        }
        else if(node.isURI()){
            String filterCond = forLoopVarName + "." + attributeName + AqlOperators.EQUALS + " '" + node.getURI() + "'";
            filterConditions.add(filterCond);
        }
        else if(node.isBlank()){
            //blank nodes act as variables, not much difference other than that the same blank node label cannot be used
            //in two different basic graph patterns in the same query. But it's unclear whether blank nodes can still be projecte... maybe consider that they aren't for this impl
            //indicated by either the label form, such as "_:abc", or the abbreviated form "[]"
            //TODO if blank node doesn't have label, use node.getBlankNodeId();
            //TODO also check that blank node label wasn't already used in some other graph pattern
            System.out.println("LET " + node.getBlankNodeLabel() + " = " + forLoopVarName + "." + attributeName);
        }
        else if(node.isLiteral()){
            String filterCond = ProcessLiteralNode(node);
        }
    }

    /*private String ProcessTripleObject(Node node){
        if(node.isLiteral()){
            return ProcessLiteralNode(node);
        }
        else {
            //else handle resource
            return ProcessTripleResource(node);
        }
    }

    private String ProcessTripleResource(Node node){
        if(node.isVariable()) {
            System.out.println("LET " + node.getName() + " = " + forLoopVarName +".s");
        }
        else if(node.isURI()){
            String filterCond = forLoopVarName + ".s " + AqlOperators.EQUALS + " '" + node.getURI() + "'";
            filterConditions.add(filterCond);
        }
        else if(node.isBlank()){
            node.getBlankNodeLabel();
            node.getBlankNodeId();
        }
        else{
            //TODO throw exception - subject is not expected type.. ORRR it just won't return results lol so let it be.. but a Virtuoso sparql endpoint would error on this...
        }
    }*/

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
        //String jsonObject = "{";
        String filterConds = "(";
        for(Var var : vars){
            //jsonObject += "\"" + var.getVarName() + "\" : " + binding.get(var).toString();
            System.out.print(var.getVarName() + " " + AqlOperators.EQUALS + " " + binding.get(var).toString());
            if(vars.get(vars.size() -1 ) != var){
                filterConds += " " + AqlOperators.AND + " ";
                //jsonObject += ",";
            }
        }
        //jsonObject += "}";
        //return jsonObject;
        filterConds += ")";
        return filterConds;
    }

    //TODO consider removing this - instead create a custom ExprVisitor and get the generated AQL equvialent expression from it
    //Expr is just an interface and there are other classes that implement it and represent operators - refer to https://jena.apache.org/documentation/javadoc/arq/org/apache/jena/sparql/expr/Expr.html
    public static String ProcessExpr(Expr expr){
        String aqlExpr = "";

        if(expr instanceof ExprFunction2){
            ExprFunction2 exprFunction2 = (ExprFunction2)expr;
            //TODO need a Map mapping ARQ expr/function symbol or class name to AQL ops/functions
            System.out.println(exprFunction2.getFunctionSymbol().getSymbol());
        }

        return expr.toString();
    }

    public static String ProcessLiteralNode(Node literal){
        //important to compare to data type in Arango object here
        RDFDatatype datatype = literal.getLiteralDatatype();
        literal.getLiteralDatatypeURI();
        //TODO add filter clause matching data type uri
        if (datatype instanceof RDFLangString) {
            literal.getLiteralLanguage();
            //TODO add filter clause matching language
        }

        //TODO add filter for value - not sure which of these 2 below methods to call to get the value - refer to https://www.w3.org/TR/sparql11-query/#matchingRDFLiterals
        //would probably be easier to use the lexical form everywhere.. that way I don't have to parse by type
        literal.getLiteralValue();
        literal.getLiteralLexicalForm();

        return "";
    }

}
