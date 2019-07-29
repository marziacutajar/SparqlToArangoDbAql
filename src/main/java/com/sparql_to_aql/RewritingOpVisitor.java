package com.sparql_to_aql;

//class used for rewriting of SPARQL algebra expression to AQL
//TODO decide whether we will "rewrite" the expression to use algebra operators that are
//more AQL specific (by creating custom Op and sub operators for AQL), or whether we will
//translate the SPARQL algebra expressions directly to an AQL query (would be hard to re-optimise such a query though..)
//TODO consider creating a new OpAssign operator for representing LET statement in AQL.. would be helpful when joining etc..
//TODO also consider creating a new OpCollect operator
import com.sparql_to_aql.constants.ArangoDatabaseSettings;
import com.sparql_to_aql.constants.NodeRole;
import com.sparql_to_aql.entities.algebra.OpDistinctProject;
import com.sparql_to_aql.entities.algebra.aql.AqlOp;
import com.sparql_to_aql.entities.algebra.transformers.OpDistinctTransformer;
import com.sparql_to_aql.utils.RewritingUtils;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.SortCondition;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprFunction;
import org.apache.jena.sparql.sse.SSE;

import java.util.*;
import java.util.stream.Collectors;

//TODO could possibly use WalkerVisitor (to visit both Ops and Exprs in the same class)
//TODO If rewriting to the actual AQL query, maybe use StringBuilder (refer to https://www.codeproject.com/Articles/1241363/Expression-Tree-Traversal-Via-Visitor-Pattern-in-P)
public class RewritingOpVisitor extends RewritingOpVisitorBase {

    //TODO build Aql query expression tree using below if we're gonna have seperate AQL algebra structure
    //private AqlOp _aqlAlgebraQueryExpression;

    //This method is to be called after the visitor has been used
    /*public AqlOp GetAqlAlgebraQueryExpression()
    {
        return _aqlAlgebraQueryExpression;
    }*/

    private int forLoopCounter = 0;
    //Keep track if which variables have already been bound in outer for loops
    private static Map<String, List<String>> usedVariablesByForLoopItem = new HashMap<>();

    //TODO consider the possibility of replacing BGP with more than 1 triple pattern into multiple joins of triple patterns (remove bgps)
    //each Triple should maybe be translated into a custom OpLoop operator in AQL
    @Override
    public void visit(OpBGP opBpg){
        for(Triple triple : opBpg.getPattern().getList()){
            //TODO consider moving this logic to a ProcessTriple method in RewritingOpVisitor
            //TODO below vars should be private global in this class for use in other visitor methods somehow
            List<String> usedVars = new ArrayList<>();
            List<String> filterConditions = new ArrayList<>();
            forLoopCounter++;
            String forloopvarname = "forloop" + forLoopCounter + "item";
            System.out.println("FOR " + forloopvarname + " IN " + ArangoDatabaseSettings.rdfCollectionName);

            //TODO also keep list of filter clauses per triple..
            RewritingUtils.ProcessTripleNode(triple.getSubject(), NodeRole.SUBJECT, forloopvarname, usedVars, filterConditions);
            RewritingUtils.ProcessTripleNode(triple.getPredicate(), NodeRole.PREDICATE, forloopvarname, usedVars, filterConditions);
            RewritingUtils.ProcessTripleNode(triple.getObject(), NodeRole.OBJECT, forloopvarname, usedVars, filterConditions);
            usedVariablesByForLoopItem.put(forloopvarname, usedVars);
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
        //TODO what if there are variables on the righthand side not on the left side.. how to handle this?
        //if there are no shared variables, do nothing (no matching bindings)
        //IMPORTANT: In MINUS operator we only consider variable bindings ex. if we have MINUS(<http://a> <http://b> <http://c>) and this triple is
        //in the results of the lefthand side, that triple won't be removed because there are no bindings
        //there must be at least one common variable between left and right side
        System.out.println("LET minus_results = MINUS(left_side_results_array, right_side_results_array)");
    }

    @Override
    public void visit(OpFilter opFilter){
        //TODO iterate over expressions, add filter conditions in AQL format to some List<String> for concatenating later
        System.out.print("FILTER ");
        for(Iterator<Expr> i = opFilter.getExprs().iterator(); i.hasNext();){
            RewritingUtils.ProcessExpr(i.next());
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
                //TODO if there's a sort clause in the expression, this will have to come between the return and collect statements
                // OR just use LET = the whole query and add sort on it..
                //SELECT DISTINCT WITH >1 VAR = COLLECT in AQL... TODO mention this in thesis writeup in AQL algebra??
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

}
