package com.sparql_to_aql.utils;

import com.sparql_to_aql.constants.ArangoAttributes;
import com.sparql_to_aql.constants.NodeRole;
import com.sparql_to_aql.constants.RdfObjectTypes;
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RewritingUtils {
    public static void ProcessTripleNode(Node node, NodeRole role, String forLoopVarName, List<String> usedVars, List<String> filterConditions){
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

        //DEPENDS IF YOU CAN HAVE MIX OF LET AND FILTER AFTER EACH OTHER IN THE SAME FOR LOOP - I think so.. refer to https://www.arangodb.com/docs/stable/aql/operations-filter.html
        if(node.isVariable()) {
            String var_name = node.getName();
            if(usedVars.contains(var_name)){
                //node was already bound in another triple, add a filter condition instead
                //TODO we might have to use AQL MATCHES function here to make sure objects are identical
                filterConditions.add(forLoopVarName + "." + attributeName + AqlOperators.EQUALS + var_name);
            }
            else {
                System.out.println("LET " + node.getName() + " = " + forLoopVarName + "." + attributeName);
                //add variable to list of already used/bound vars
                usedVars.add(node.getName());
            }
        }
        else if(node.isURI()){
            filterConditions.add(forLoopVarName + "." + attributeName + "." + ArangoAttributes.TYPE + AqlOperators.EQUALS + AqlUtils.quoteString(RdfObjectTypes.IRI));
            filterConditions.add(forLoopVarName + "." + attributeName + "." + ArangoAttributes.VALUE + AqlOperators.EQUALS + AqlUtils.quoteString(node.getURI()));
        }
        else if(node.isBlank()){
            //blank nodes act as variables, not much difference other than that the same blank node label cannot be used
            //in two different basic graph patterns in the same query. But it's unclear whether blank nodes can still be projecte... maybe consider that they aren't for this impl
            //indicated by either the label form, such as "_:abc", or the abbreviated form "[]"
            //TODO if blank node doesn't have label, use node.getBlankNodeId();
            System.out.println("ID " + node.getBlankNodeId());
            //TODO also check that blank node label wasn't already used in some other graph pattern
            System.out.println("LET " + node.getBlankNodeLabel() + " = " + forLoopVarName + "." + attributeName);
        }
        else if(node.isLiteral()){
            filterConditions.addAll(ProcessLiteralNode(node));
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
        //the FILTER commands should be added if there is a JOIN clause between a Bindings table and some other op..
        //thus the functionality below could be changed to represent the Table in Arango (ex. array of objects with the bound vars..)
        //String bindingsInAql = "LET bindings = [";
        String filterConds = "";
        List<Var> vars = table.getVars();
        for (Iterator<Binding> i = table.rows(); i.hasNext();){
            //bindingsInAql += ProcessBinding(i.next(), vars);
            filterConds += ProcessBinding(i.next(), vars);
            if(i.hasNext()){
                //bindingsInAql += ",";
                filterConds += " " + AqlOperators.OR + " ";
            }
        }
        //bindingsInAql += "]";
        //System.out.println(bindingsInAql);
        System.out.println(filterConds);
    }

    public static String ProcessBinding(Binding binding, List<Var> vars){
        //String jsonObject = "{";
        int undefinedVarsAmount = 0;
        String filterConds = "(";
        for(Var var : vars){
            //jsonObject += "\"" + var.getVarName() + "\" : " + binding.get(var).toString();
            Node value = binding.get(var);
            if(value == null) {
                //the variable is bound to an undefined value, that is the value of that variable can be anything in this binding case
                //if all values in one binding (one row of the table) are all null (UNDEF), then result set shouldn't be filtered
                undefinedVarsAmount++;
                if(undefinedVarsAmount == vars.size())
                    filterConds += "true";

                continue;
            }

            //TODO consider whether the binding is to a literal, or uri.. if literal what data type it has, etc...
            filterConds += var.getVarName() + " " + AqlOperators.EQUALS + " " + value.toString();
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

    public static List<String> ProcessLiteralNode(Node literal){
        //important to compare to data type in Arango object here
        List<String> filterConds = new ArrayList<>();
        filterConds.add("var_name_here" + "." + ArangoAttributes.OBJECT + "." + ArangoAttributes.TYPE + AqlOperators.EQUALS + AqlUtils.quoteString(RdfObjectTypes.LITERAL));
        RDFDatatype datatype = literal.getLiteralDatatype();
        filterConds.add("var_name_here" + "." + ArangoAttributes.OBJECT + "." + ArangoAttributes.LITERAL_DATA_TYPE + AqlOperators.EQUALS + AqlUtils.quoteString(datatype.getURI()));

        if (datatype instanceof RDFLangString) {
            filterConds.add("var_name_here" + "." + ArangoAttributes.OBJECT + "." + ArangoAttributes.LITERAL_LANGUAGE + AqlOperators.EQUALS + AqlUtils.quoteString(literal.getLiteralLanguage()));
        }

        //deiced which of these 2 below methods to call to get the value - refer to https://www.w3.org/TR/sparql11-query/#matchingRDFLiterals
        //would probably be easier to use the lexical form everywhere.. that way I don't have to parse by type.. although when showing results to user we'll have to customize their displays according to the type..
        //literal.getLiteralValue();
        //TODO using the lexical form won't work when we want to apply math or string functions to values in AQL!
        filterConds.add("var_name_here" + "." + ArangoAttributes.OBJECT + "." + ArangoAttributes.VALUE + AqlOperators.EQUALS + AqlUtils.quoteString(literal.getLiteralLexicalForm()));

        return filterConds;
    }

}
