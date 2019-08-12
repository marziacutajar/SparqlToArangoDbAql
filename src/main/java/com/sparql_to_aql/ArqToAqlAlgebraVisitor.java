package com.sparql_to_aql;

//class used for rewriting of SPARQL algebra expression to AQL
//TODO decide whether we will "rewrite" the expression to use algebra operators that are
//more AQL specific (by creating custom Op and sub operators for AQL), or whether we will
//translate the SPARQL algebra expressions directly to an AQL query (would be hard to re-optimise such a query though..)
//TODO consider creating a new OpAssign operator for representing LET statement in AQL.. would be helpful when joining etc..
//TODO also consider creating a new OpCollect operator
import com.sparql_to_aql.constants.ArangoAttributes;
import com.sparql_to_aql.constants.NodeRole;
import com.sparql_to_aql.constants.RdfObjectTypes;
import com.sparql_to_aql.constants.arangodb.AqlOperators;
import com.sparql_to_aql.entities.algebra.OpDistinctProject;
import com.sparql_to_aql.entities.algebra.aql.operators.Op;
import com.sparql_to_aql.entities.algebra.aql.operators.OpFor;
import com.sparql_to_aql.utils.AqlUtils;
import com.sparql_to_aql.utils.RewritingUtils;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.impl.RDFLangString;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.SortCondition;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.expr.Expr;

import java.util.*;
import java.util.stream.Collectors;

//TODO could possibly use WalkerVisitor (to visit both Ops and Exprs in the same class)
//TODO If rewriting to the actual AQL query, maybe use StringBuilder (refer to https://www.codeproject.com/Articles/1241363/Expression-Tree-Traversal-Via-Visitor-Pattern-in-P)
public class ArqToAqlAlgebraVisitor extends RewritingOpVisitorBase {

    //TODO build Aql query expression tree using below if we're gonna have seperate AQL algebra structure
    private Op _aqlAlgebraQueryExpression;

    //This method is to be called after the visitor has been used
    /*public Op GetAqlAlgebraQueryExpression()
    {
        return _aqlAlgebraQueryExpression;
    }*/

    private int forLoopCounter = 0;
    //Keep track if which variables have already been bound in outer for loops
    //TODO or create visit methods that return the result and create a custom walker that uses the results to compile the final AQL expression
    // similar to how transformations work.. either have to add a new method public Op visit(OpType_here op) in each Op class OR make use of transform methods...latter could work
    // but even if I use a transform visitor.. I will have to extend all Op classes with what I need to store in them
    private static Map<String, List<String>> usedVariablesByForLoopItem = new HashMap<>();

    //TODO consider the possibility of replacing BGP with more than 1 triple pattern into multiple joins of triple patterns (remove bgps)
    //each Triple should maybe be translated into a custom OpLoop operator in AQL
    @Override
    public void visit(OpBGP opBpg){
        for(Triple triple : opBpg.getPattern().getList()){
            List<String> usedVars = new ArrayList<>();
            //keep list of FILTER clauses per triple
            List<String> filterConditions = new ArrayList<>();
            //keep list of LET clauses per triple
            List<String> assignments = new ArrayList<>();

            String forloopvarname = getNewVarName();
            Op aqlOp = new OpFor(forloopvarname, "collectionOrVarName_here");

            ProcessTripleNode(triple.getSubject(), NodeRole.SUBJECT, aqlOp, forloopvarname, filterConditions, assignments, usedVars);
            ProcessTripleNode(triple.getPredicate(), NodeRole.PREDICATE, aqlOp, forloopvarname, filterConditions, assignments, usedVars);
            ProcessTripleNode(triple.getObject(), NodeRole.OBJECT, aqlOp, forloopvarname, filterConditions, assignments, usedVars);
            usedVariablesByForLoopItem.put(forloopvarname, usedVars);
            filterConditions.forEach(f -> System.out.println(f));
            assignments.forEach(a -> System.out.println(a));
        }
    }

    /*TODO consider the possibility of transforming all OpTriple instances to OpBgp so we can avoid repeating code and remove this visitor method.. or all OpBGPs to OpTriples
    @Override
    public void visit(OpTriple opTriple){
        Triple triple = opTriple.getTriple();

        System.out.println("TRIPLE HERE");
    }*/

    /*TODO decide if I need this
    @Override
    public void visit(OpQuad opQuad){
    }*/

    @Override
    public void visit(OpJoin opJoin){
        System.out.println("Entering join");
        //TODO Here we'll need to use some current list of common variables between the resulting "bgps" that must be joined
        //OR use ATTRIBUTES function in AQL over both of them to join on the common attrs found
        //and create a FILTER statement with them and then merge variables in both
        //TODO check if the left or right side is a table, in which case we need to cater for that..
        if(opJoin.getRight() instanceof OpTable){
            //TODO use variable name used in for loop in the filter conds if applicable..
            // possibly instead of passing var name directly to methods for usage, put a placeholder and then replace
            // all placeholders with the actual variable names after whole query construction (by keeping a map of varname to op)
            OpTable opTable = (OpTable) opJoin.getRight();
            RewritingUtils.ProcessBindingsTable(opTable.getTable());
        }
        else{
            //System.out.println("FILTER left_side.var = right_side.var");
            //System.out.println("RETURN { var1 = left_side.var1, var2 = leftside.var2, var3 = rightside.var3}");
        }
    }

    @Override
    public void visit(OpLeftJoin opLeftJoin){
        //TODO take array results of left and right subqueries
        //add a filter on the right side results to make sure common variables match to those on the left,
        //opLeftJoin.getExprs();
        System.out.println("FOR x IN left_results");
        System.out.println("LET filtered_right_side = (FOR y IN right_results FILTER common_filter_expr_here RETURN y)");
        System.out.println("FOR right_result_to_join IN (LENGTH(filtered_right_side) > 0 ? filtered_right_side : [{}])");
        System.out.println("RETURN { left: x, right: right_result_to_join}");
    }

    @Override
    public void visit(OpMinus opMinus){
        //call MINUS function on left and right sides, assign to a variable using LET
        //TODO what if there are variables on the righthand side not on the left side.. how to handle this? .. I think you just ignore them..
        //if there are no shared variables, do nothing (no matching bindings)
        //IMPORTANT: In MINUS operator we only consider variable bindings ex. if we have MINUS(<http://a> <http://b> <http://c>) and this triple is
        //in the results of the lefthand side, that triple won't be removed because there are no bindings
        //there must be at least one common variable between left and right side
        System.out.println("LET minus_results = MINUS(left_side_results_array, right_side_results_array)");
    }

    @Override
    public void visit(OpFilter opFilter){
        System.out.print("FILTER ");
        //iterate over expressions, add filter conditions in AQL format to list for concatenating later
        List<String> filterConds = new ArrayList<>();
        for(Iterator<Expr> i = opFilter.getExprs().iterator(); i.hasNext();){
            filterConds.add(RewritingUtils.ProcessExpr(i.next()));
        }
    }

    @Override
    public void visit(OpExtend opExtend){
        List<String> extendExpressions = new ArrayList<>();
        VarExprList varExprList = opExtend.getVarExprList();
        varExprList.forEachVarExpr((v,e) -> extendExpressions.add("LET " + v.getVarName() + " = " + RewritingUtils.ProcessExpr(e)));
    }

    @Override
    public void visit(OpUnion opUnion){
        //TODO get the subquery for the left and right of the union
        System.out.print("LET unionResult = UNION(left_result_here, right_result_here)");
    }

    @Override
    public void visit(OpGraph opGraph){
        //TODO Add extra filter condition to list of filter clauses when we have one..
        // ACTUALLY - best option would be to use a transformer to move the graph operator down over every bgp or every triple (quad form) in its subtree and then remove this outer one
        Node graphNode = opGraph.getNode();
        if(graphNode.isVariable()){
            String forloopvarname = "forloop" + forLoopCounter + "item";
            //else here bind the variable.. maybe we can call Visit(OpExtend) instead here by replacing it?
            System.out.println("LET " + graphNode.getName() + " = " + forloopvarname + ".g");
        }
        else if(graphNode.isURI()){
            System.out.println("FILTER item_name_here.g = " + graphNode.getURI());
        }
    }

    @Override
    public void visit(OpProject opProject){
        String collectStmt = "";
        String returnStatement = "RETURN ";
        List<Var> projectableVars = opProject.getVars();

        if(opProject instanceof OpDistinctProject){
            if(projectableVars.size() == 1){
                returnStatement += "DISTINCT ";
            }
            else{
                //SELECT DISTINCT WITH >1 VAR = COLLECT in AQL... consider mentioning this in thesis writeup in AQL algebra
                collectStmt = "COLLECT ";
                for(Var v: projectableVars){
                    //Add each var to collect clause
                    //TODO remember that when assigning it we have to use for loop over the query results and use the forloop item name
                    collectStmt += v.getVarName() + " = " + v.getVarName();
                    if(projectableVars.get(projectableVars.size()-1) != v)
                        collectStmt += ", ";
                }
                collectStmt += "\n";
            }
        }

        String delimitedVariables = projectableVars.stream().map(v -> v.getVarName())
                .collect(Collectors.joining( ", " ));
        returnStatement += delimitedVariables;
        System.out.println(collectStmt + returnStatement);
    }

    @Override
    public void visit(OpOrder opOrder) {
        //TODO use LET = the whole query, then add sort on it and return every row
        List<SortCondition> sortConditionList = opOrder.getConditions();
        String[] conditions = new String[sortConditionList.size()];

        for (int i= 0; i < sortConditionList.size(); i++) {
            SortCondition currCond = sortConditionList.get(i);
            //direction = 1 if ASC, -1 if DESC, -2 if unspecified (default asc)
            String direction = currCond.getDirection() == -1 ? "DESC" : "ASC";
            conditions[i] = currCond.getExpression() + " " + direction;
        }

        System.out.println("SORT " + String.join(", ", conditions));
    }

    /*@Override
    public void visit(OpDistinct opDistinct){
        //or if we have more than one distinct variable we need to use COLLECT
        if(opDistinct.getSubOp() instanceof OpProject){
            OpProject projectNode = (OpProject) opDistinct.getSubOp();
            if(projectNode.getVars().size() == 1){
                //add distinct to return statement
            }
        }
        //one option is to keep a list of projected variables in this class, check if the num of vars is 1, in which case
        //use RETURN DISTINCT, else introduce COLLECT statement
    }*/

    //New visit method for my custom OpDistinctProject class
    /*public void visit(OpDistinctProject opDistinctProject){
        System.out
    }*/

    @Override
    public void visit(OpGroup opGroup){
        //TODO group, and consider mathematical expressions
        //String collectStmt = "COLLECT ";
        List<String> collectClauses = new ArrayList<>();
        VarExprList varExprList = opGroup.getGroupVars();
        varExprList.forEachVarExpr((v,e) -> collectClauses.add(v.getVarName() + " = " + RewritingUtils.ProcessExpr(e)));
        //System.out.println(collectStmt);
    }

    @Override
    public void visit(OpSlice opSlice){
        Long offset = opSlice.getStart();
        Long limit = opSlice.getLength();

        if(offset < 1){
            System.out.println("LIMIT " + limit);
            return;
        }

        System.out.println("LIMIT " + offset + ", " + limit);
    }

    /*@Override
    public void visit(OpTopN opTopN){
        //this operator is only present if we use TransformTopN optimizer to change from OpSlice to OpTopN..
        //This operator contains limit + order by for better performance
        //https://jena.apache.org/documentation/javadoc/arq/org/apache/jena/sparql/algebra/op/OpTopN.html

        List<SortCondition> sortConditionList = opTopN.getConditions();
        String[] conditions = new String[sortConditionList.size()];

        for (int i= 0; i < sortConditionList.size(); i++) {
            SortCondition currCond = sortConditionList.get(i);
            //direction = 1 if ASC, -1 if DESC, -2 if unspecified (default asc)
            String direction = currCond.getDirection() == -1 ? "DESC" : "ASC";
            conditions[i] = currCond.getExpression() + " " + direction;
        }

        System.out.println("SORT " + String.join(", ", conditions));

        System.out.println("LIMIT " + opTopN.getLimit());
    }*/

    /*public void visit(OpTable opTable){
        //TODO I think we need to add a filter clause such as (?var1 = val_1 && ?var2 = val_2) || (?var1 = val_3 && ?var2 = val_3) etc...
        //If we have a join to a table.. we could skip processing this operator and process it in the join.. might be more efficient actually then having to process it at both points
        RewritingUtils.ProcessBindingsTable(opTable.getTable());
    }*/

    //TODO decide whether to add visit methods for OpList, OpPath and others.. or whether they'll be unsupported

    private String getNewVarName(){
        forLoopCounter++;
        return "forloop" + forLoopCounter + "item";
    }

    public static void ProcessTripleNode(Node node, NodeRole role, Op currOp, String forLoopVarName, List<String> filterConditions, List<String> assignments, List<String> usedVars){
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

        //We can have a mixture of LET and FILTER statements after each other - refer to https://www.arangodb.com/docs/stable/aql/operations-filter.html
        if(node.isVariable()) {
            String var_name = node.getName();
            if(usedVars.contains(var_name)){
                //node was already bound in another triple, add a filter condition instead
                //TODO we might have to use AQL MATCHES function here to make sure objects are identical
                Op filterOp = new com.sparql_to_aql.entities.algebra.aql.operators.OpFilter(currOp);
                filterConditions.add(forLoopVarName + "." + attributeName + AqlOperators.EQUALS + var_name);
            }
            else {
                assignments.add("LET " + node.getName() + " = " + forLoopVarName + "." + attributeName);
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
            assignments.add("LET " + node.getBlankNodeLabel() + " = " + forLoopVarName + "." + attributeName);
        }
        else if(node.isLiteral()){
            filterConditions.addAll(ProcessLiteralNode(node, forLoopVarName, filterConditions));
        }
    }

    public static List<String> ProcessLiteralNode(Node literal, String forLoopVarName, List<String> filterConditions){
        //important to compare to data type in Arango object here
        filterConditions.add(forLoopVarName + "." + ArangoAttributes.OBJECT + "." + ArangoAttributes.TYPE + AqlOperators.EQUALS + AqlUtils.quoteString(RdfObjectTypes.LITERAL));
        RDFDatatype datatype = literal.getLiteralDatatype();
        filterConditions.add(forLoopVarName + "." + ArangoAttributes.OBJECT + "." + ArangoAttributes.LITERAL_DATA_TYPE + AqlOperators.EQUALS + AqlUtils.quoteString(datatype.getURI()));

        if (datatype instanceof RDFLangString) {
            filterConditions.add(forLoopVarName + "." + ArangoAttributes.OBJECT + "." + ArangoAttributes.LITERAL_LANGUAGE + AqlOperators.EQUALS + AqlUtils.quoteString(literal.getLiteralLanguage()));
        }

        //deiced which of these 2 below methods to call to get the value - refer to https://www.w3.org/TR/sparql11-query/#matchingRDFLiterals
        //would probably be easier to use the lexical form everywhere.. that way I don't have to parse by type.. although when showing results to user we'll have to customize their displays according to the type..
        //literal.getLiteralValue();
        //TODO using the lexical form won't work when we want to apply math or string functions to values in AQL!
        filterConditions.add(forLoopVarName + "." + ArangoAttributes.OBJECT + "." + ArangoAttributes.VALUE + AqlOperators.EQUALS + AqlUtils.quoteString(literal.getLiteralLexicalForm()));

        return filterConditions;
    }
}

