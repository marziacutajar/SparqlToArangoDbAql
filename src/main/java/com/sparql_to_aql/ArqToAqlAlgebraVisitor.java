package com.sparql_to_aql;

import com.aql.algebra.operators.*;
import com.aql.algebra.operators.OpAssign;
import com.sparql_to_aql.constants.ArangoAttributes;
import com.sparql_to_aql.constants.ArangoDatabaseSettings;
import com.sparql_to_aql.constants.NodeRole;
import com.sparql_to_aql.constants.RdfObjectTypes;
import com.sparql_to_aql.entities.algebra.OpDistinctProject;
import com.aql.algebra.expressions.ExprList;
import com.aql.algebra.expressions.constants.Const_Array;
import com.aql.algebra.expressions.constants.Const_Bool;
import com.aql.algebra.expressions.constants.Const_Number;
import com.aql.algebra.expressions.constants.Const_String;
import com.aql.algebra.expressions.functions.*;
import com.sparql_to_aql.entities.algebra.OpGraphBGP;
import com.sparql_to_aql.utils.AqlUtils;
import com.sparql_to_aql.utils.MapUtils;
import com.sparql_to_aql.utils.RewritingUtils;
import com.sparql_to_aql.utils.VariableGenerator;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.SortCondition;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpMinus;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.expr.Expr;
import java.util.*;
import java.util.stream.Collectors;

//class used for rewriting of SPARQL algebra query expression to AQL algebra query expression
//translating the SPARQL algebra expressions directly to an AQL query would be hard to re-optimise
//TODO decide whether to add visit methods for OpList, OpPath and others.. or whether they'll be unsupported
public class ArqToAqlAlgebraVisitor extends RewritingOpVisitorBase {

    //private String defaultGraphCollectionOrVarName;

    //Aql query can be made of a sequence of "subqueries" and assignments, hence the list
    private List<Op> _aqlAlgebraQueryExpressionTree;

    List<String> defaultGraphNames;
    List<String> namedGraphNames;

    //This method is to be called after the visitor has been used
    public List<Op> GetAqlAlgebraQueryExpression()
    {
        return _aqlAlgebraQueryExpressionTree;
    }

    private VariableGenerator forLoopVarGenerator = new VariableGenerator("forloop", "item");
    private VariableGenerator assignmentVarGenerator = new VariableGenerator("assign", "item");

    //Keep track of which variables have already been bound (or not if optional), by mapping ARQ algebra op hashcode to the list of vars
    //the second map is used to map the sparql variable name  into the corresponding aql variable name to use (due to for loop variable names)
    private Map<Integer, Map<String, String>> boundSparqlVariablesByOp = new HashMap<>();

    //use linked list - easier to pop out and push items from front or back
    private LinkedList<Op> createdAqlOps = new LinkedList<>();

    public ArqToAqlAlgebraVisitor(List<String> defaultGraphNames, List<String> namedGraphs){
        this.defaultGraphNames = defaultGraphNames;
        this.namedGraphNames = namedGraphs;
    }

    private void AddSparqlVariablesByOp(Integer opHashCode, Map<String, String> variables){
        Map<String, String> currUsedVars = GetSparqlVariablesByOp(opHashCode);
        if(currUsedVars == null) {
            boundSparqlVariablesByOp.put(opHashCode, variables);
        }
        else {
            MapUtils.MergeMapsKeepFirstDuplicateKeyValue(currUsedVars, variables);
        }
    }

    private void SetSparqlVariablesByOp(Integer opHashCode, Map<String, String> variables){
        boundSparqlVariablesByOp.put(opHashCode, variables);
    }

    private Map<String, String> GetSparqlVariablesByOp(Integer opHashCode){
        Map<String, String> currUsedVars = boundSparqlVariablesByOp.get(opHashCode);
        if(currUsedVars == null)
            return new HashMap<>();

        return currUsedVars;
    }

    @Override
    public void visit(OpBGP opBgp){
        boolean bgpWithGraphNode = false;
        Node graphNode = null;
        if(opBgp instanceof OpGraphBGP){
            bgpWithGraphNode = true;
            OpGraphBGP graphBGP = (OpGraphBGP) opBgp;
            graphNode = graphBGP.getGraphNode();
        }

        Op currAqlOp = null;
        Map<String, String> usedVars = new HashMap<>();
        boolean firstTripleBeingProcessed = true;
        for(Triple triple : opBgp.getPattern().getList()){
            //keep list of FILTER clauses per triple
            ExprList filterConditions = new ExprList();

            String iterationVar = forLoopVarGenerator.getNew();
            Op aqlOp = new OpFor(iterationVar, com.aql.algebra.expressions.Var.alloc(ArangoDatabaseSettings.rdfCollectionName));

            //using this variable, we will make sure the graph name of every triple matching the BGP is in the same graph
            String outerGraphVarToMatch = AqlUtils.buildVar(iterationVar, ArangoAttributes.GRAPH_NAME, ArangoAttributes.VALUE);

            //if this is the first for loop and there are named graphs specified, add filters for those named graphs
            if(firstTripleBeingProcessed){
                if(bgpWithGraphNode){
                    if(graphNode.isVariable()){
                        AddGraphFilters(namedGraphNames, iterationVar, filterConditions);

                        //bind graph var
                        usedVars.put(graphNode.getName(), AqlUtils.buildVar(iterationVar, ArangoAttributes.GRAPH_NAME));
                    }
                    else{
                        //add filter with specific named graph
                        filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(outerGraphVarToMatch), new Const_String(graphNode.getURI())));
                    }
                }
                else{
                    //if there are default graphs specified, filter by those
                    if(defaultGraphNames.size() > 0){
                        AddGraphFilters(defaultGraphNames, iterationVar, filterConditions);
                    }
                }
            }
            else{
                //make sure that graph name for consecutive triples matches the one of the first triple
                filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(outerGraphVarToMatch), com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(iterationVar, ArangoAttributes.GRAPH_NAME, ArangoAttributes.VALUE))));
            }

            RewritingUtils.ProcessTripleNode(triple.getSubject(), NodeRole.SUBJECT, iterationVar, filterConditions, usedVars);
            RewritingUtils.ProcessTripleNode(triple.getPredicate(), NodeRole.PREDICATE, iterationVar, filterConditions, usedVars);
            RewritingUtils.ProcessTripleNode(triple.getObject(), NodeRole.OBJECT, iterationVar, filterConditions, usedVars);

            Op filterOp = new com.aql.algebra.operators.OpFilter(filterConditions, aqlOp);

            if(currAqlOp == null) {
                currAqlOp = filterOp;
            }
            else {
                currAqlOp = new OpNest(currAqlOp, filterOp);
            }

            firstTripleBeingProcessed = false;
        }

        //add used vars in bgp to list
        SetSparqlVariablesByOp(opBgp.hashCode(), usedVars);
        createdAqlOps.add(currAqlOp);
    }

    @Override
    public void visit(OpJoin opJoin){
        System.out.println("Entering join");
        Op opToJoin1 = createdAqlOps.removeFirst();
        //whether we use LET stsms or not here depends if the ops being joined include a projection or not
        boolean joinToValuesTable = false;
        OpTable opTable = null;
        Map<String, String> boundVariablesInOp1ToJoin = new HashMap<>();

        //if one side of the join is a table, cater for that
        if(opJoin.getLeft() instanceof OpTable){
            joinToValuesTable = true;
            opTable = (OpTable) opJoin.getLeft();
            boundVariablesInOp1ToJoin = GetSparqlVariablesByOp(opJoin.getRight().hashCode());
        }
        else if(opJoin.getRight() instanceof OpTable){
            joinToValuesTable = true;
            opTable = (OpTable) opJoin.getRight();
            boundVariablesInOp1ToJoin = GetSparqlVariablesByOp(opJoin.getLeft().hashCode());
        }

        if(opToJoin1 instanceof com.aql.algebra.operators.OpProject){
            opToJoin1 = AddNewAssignmentAndLoop(opToJoin1, boundVariablesInOp1ToJoin);
        }

        if(joinToValuesTable){
            opToJoin1 = new com.aql.algebra.operators.OpFilter(RewritingUtils.ProcessBindingsTableJoin(opTable.getTable(), boundVariablesInOp1ToJoin), opToJoin1);
            SetSparqlVariablesByOp(opJoin.hashCode(), boundVariablesInOp1ToJoin);
            createdAqlOps.add(opToJoin1);
        }
        else{
            Op opToJoin2 = createdAqlOps.removeFirst();
            Map<String, String> boundVariablesInOp2ToJoin = GetSparqlVariablesByOp(opJoin.getRight().hashCode());

            if(opToJoin2 instanceof com.aql.algebra.operators.OpProject){
                opToJoin2 = AddNewAssignmentAndLoop(opToJoin1, boundVariablesInOp2ToJoin);
            }

            //use list of common variables between the resulting "bgps" that must be joined
            //also add used vars in join to sparqlVariablesByOp
            AddSparqlVariablesByOp(opJoin.hashCode(), boundVariablesInOp1ToJoin);
            AddSparqlVariablesByOp(opJoin.hashCode(), boundVariablesInOp2ToJoin);

            Set<String> commonVars = MapUtils.GetCommonMapKeys(boundVariablesInOp1ToJoin, boundVariablesInOp2ToJoin);

            ExprList filtersExprs = new ExprList();
            for (String commonVar: commonVars){
                filtersExprs.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(boundVariablesInOp1ToJoin.get(commonVar))), com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(boundVariablesInOp2ToJoin.get(commonVar)))));
            }

            //nest one for loop in the other and add filter statements
            opToJoin1 = new OpNest(opToJoin1, new com.aql.algebra.operators.OpFilter(filtersExprs, opToJoin2));
            createdAqlOps.add(opToJoin1);
        }
    }

    @Override
    public void visit(OpLeftJoin opLeftJoin){
        //TODO take array results of left and right subqueries
        //add a filter on the right side results to make sure common variables match to those on the left,
        //opLeftJoin.getExprs();
        String outerLoopVarName = forLoopVarGenerator.getNew();
        new OpFor(outerLoopVarName, com.aql.algebra.expressions.Var.alloc("left_results"));
        String subqueryVar = forLoopVarGenerator.getNew();
        Op subquery = new OpFor(subqueryVar, com.aql.algebra.expressions.Var.alloc("right_results"));
        ExprList filterExprs = new ExprList();
        //TODO add common filter exprs here - use var names created
        subquery = com.aql.algebra.operators.OpFilter.filterBy(filterExprs, subquery);
        subquery = new com.aql.algebra.operators.OpProject(subquery, com.aql.algebra.expressions.Var.alloc(subqueryVar), false);
        new com.aql.algebra.operators.OpAssign("filtered_right_side", subquery);

        Op subquery2 = new OpFor("right_result_to_join", new Expr_Conditional(new Expr_GreaterThan(new Expr_Length(com.aql.algebra.expressions.Var.alloc("filtered_right_side")), new Const_Number(0)), com.aql.algebra.expressions.Var.alloc("filtered_right_side"), new Const_Array(null)));

        //TODO do below
        //subquery2 = new com.aql.algebra.operators.OpProject(subquery2, new VarExpr());
        //System.out.println("FOR right_result_to_join IN (LENGTH(filtered_right_side) > 0 ? filtered_right_side : [{}])");
        //System.out.println("RETURN { left: x, right: right_result_to_join}");
        //TODO consider using MERGE function...
    }

    //TODO what if instead of an OpMinus op in AQL we use a JOIN with not equals filter conditions??
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
        //add filter operator over current op
        Op currOp = createdAqlOps.removeLast();
        Map<String, String> boundVars = GetSparqlVariablesByOp(opFilter.getSubOp().hashCode());
        //iterate over expressions, add filter conditions in AQL format to list for concatenating later
        ExprList filterConds = new ExprList();

        for(Iterator<Expr> i = opFilter.getExprs().iterator(); i.hasNext();){
            filterConds.add(RewritingUtils.ProcessExpr(i.next(), boundVars));
        }

        createdAqlOps.add(new com.aql.algebra.operators.OpFilter(filterConds, currOp));
        SetSparqlVariablesByOp(opFilter.hashCode(), boundVars);
    }

    @Override
    public void visit(OpExtend opExtend){
        Op currOp = createdAqlOps.removeLast();

        VarExprList varExprList = opExtend.getVarExprList();

        Map<String, String> prevBoundVars = GetSparqlVariablesByOp(opExtend.getSubOp().hashCode());

        List<com.aql.algebra.operators.OpAssign> assignmentExprs = new ArrayList<>();
        varExprList.forEachVarExpr((v,e) -> assignmentExprs.add(new com.aql.algebra.operators.OpAssign(v.getVarName(), RewritingUtils.ProcessExpr(e, prevBoundVars))));
        //nest assignments into current op or extend current op

        currOp = new com.aql.algebra.operators.OpExtend(currOp, assignmentExprs);
        createdAqlOps.add(currOp);

        //add variables to sparqlVariablesByOp
        Map<String, String> newBoundVars = new HashMap<>();
        varExprList.forEachVar(v -> newBoundVars.put(v.getName(), v.getName()));
        AddSparqlVariablesByOp(opExtend.hashCode(), newBoundVars);
        AddSparqlVariablesByOp(opExtend.hashCode(), prevBoundVars);
    }

    @Override
    public void visit(OpUnion opUnion){
        //how we perform this operation depends if the union is between subqueries that have a projection or not
        Op leftOp = createdAqlOps.removeFirst();
        Op rightOp = createdAqlOps.removeFirst();

        Map<String, String> leftBoundVars = GetSparqlVariablesByOp(opUnion.getLeft().hashCode());
        Map<String, String> rightBoundVars = GetSparqlVariablesByOp(opUnion.getRight().hashCode());

        if(!(leftOp instanceof com.aql.algebra.operators.OpProject)){
            leftOp = new com.aql.algebra.operators.OpProject(leftOp, RewritingUtils.CreateVarExprList(leftBoundVars), false);
        }

        if(!(rightOp instanceof com.aql.algebra.operators.OpProject)){
            rightOp = new com.aql.algebra.operators.OpProject(rightOp, RewritingUtils.CreateVarExprList(rightBoundVars), false);
        }

        AddNewAssignment(leftOp);
        String leftAssignVar = assignmentVarGenerator.getCurrent();
        AddNewAssignment(rightOp);
        String rightAssignVar = assignmentVarGenerator.getCurrent();

        Map<String, String> allBoundVars = MapUtils.MergeMapsKeepFirstDuplicateKeyValue(leftBoundVars, rightBoundVars);
        //TODO consider using APPEND operator instead of UNION in AQL to keep any sorting order applied before
        //System.out.print("LET unionResult = UNION(left_result_here, right_result_here)");
        createdAqlOps.push(AddNewAssignmentAndLoop(new Expr_Union(com.aql.algebra.expressions.Var.alloc(leftAssignVar), com.aql.algebra.expressions.Var.alloc(rightAssignVar)), allBoundVars));
        AddSparqlVariablesByOp(opUnion.hashCode(), allBoundVars);
    }

    @Override
    public void visit(OpProject opProject){
        boolean useDistinct = false;

        Op currOp = createdAqlOps.removeLast();
        List<Var> projectableVars = opProject.getVars();

        if(opProject instanceof OpDistinctProject){
            if(projectableVars.size() == 1){
                useDistinct = true;
            }
            else{
                //SELECT DISTINCT WITH >1 VAR = COLLECT in AQL... consider mentioning this in thesis writeup in AQL algebra

                //apply collect stmt over current projectionSubOp
                currOp = new OpCollect(currOp, RewritingUtils.CreateVarExprList(projectableVars, boundSparqlVariablesByOp.get(opProject.getSubOp().hashCode())), null);
            }
        }

        List<com.aql.algebra.expressions.Expr> returnVariables = projectableVars.stream().map(v -> com.aql.algebra.expressions.Var.alloc(v.getVarName()))
                .collect(Collectors.toList());

        Op returnStmt = new com.aql.algebra.operators.OpProject(currOp, returnVariables, useDistinct);

        Map<String, String> projectedAqlVars = new HashMap<>();
        projectableVars.stream().map(v -> projectedAqlVars.put(v.getVarName(), v.getVarName()));
        boundSparqlVariablesByOp.put(opProject.hashCode(), projectedAqlVars);

        createdAqlOps.add(returnStmt);
    }

    @Override
    public void visit(OpOrder opOrder) {
        Op orderSubOp = createdAqlOps.removeLast();

        List<SortCondition> sortConditionList = opOrder.getConditions();
        List<com.aql.algebra.SortCondition> aqlSortConds = new ArrayList<>();

        Map<String, String> boundVars = boundSparqlVariablesByOp.get(opOrder.getSubOp().hashCode());

        for (int i= 0; i < sortConditionList.size(); i++) {
            SortCondition currCond = sortConditionList.get(i);
            //direction = 1 if ASC, -1 if DESC, -2 if unspecified (default asc)
            com.aql.algebra.SortCondition.Direction direction = currCond.getDirection() == -1 ? com.aql.algebra.SortCondition.Direction.DESC : com.aql.algebra.SortCondition.Direction.ASC;
            //TODO here we're assuming expr is definitely a variable.. would be better to use expression visitor and get resulting AQL expression from it.. imp to also use boundVars map here
            aqlSortConds.add(new com.aql.algebra.SortCondition(com.aql.algebra.expressions.Var.alloc(boundVars.get(currCond.getExpression().getVarName())), direction));
        }

        OpSort aqlSort = new OpSort(orderSubOp, aqlSortConds);
        createdAqlOps.add(aqlSort);
        SetSparqlVariablesByOp(opOrder.hashCode(), boundVars);
    }

    @Override
    public void visit(OpSlice opSlice){
        Op currOp = createdAqlOps.removeLast();

        createdAqlOps.add(new OpLimit(currOp, opSlice.getStart(), opSlice.getLength()));
    }

    private void AddGraphFilters(List<String> graphNames, String forLoopVarName, ExprList filterConditions){
        com.aql.algebra.expressions.Expr filterExpr = null;

        //add filters for default or named graphs
        for(String g: graphNames){
            com.aql.algebra.expressions.Expr currExpr = new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(forLoopVarName, ArangoAttributes.GRAPH_NAME, ArangoAttributes.VALUE)), new Const_String(g));

            if(filterExpr == null){
                filterExpr = currExpr;
            }
            else{
                filterExpr = new Expr_LogicalOr(filterExpr, currExpr);
            }
        }

        filterConditions.add(filterExpr);
    }

    private void AddNewAssignment(Op opToAssign){
        _aqlAlgebraQueryExpressionTree.add(new OpAssign(assignmentVarGenerator.getNew(), opToAssign));
    }

    private void AddNewAssignment(com.aql.algebra.expressions.Expr exprToAssign){
        _aqlAlgebraQueryExpressionTree.add(new OpAssign(assignmentVarGenerator.getNew(), exprToAssign));
    }

    private Op AddNewAssignmentAndLoop(Op opToAssign, Map<String, String> boundVars){
        //create for loop over query that already had projection by using let stmt
        //Add let stmt to our main query structure
        _aqlAlgebraQueryExpressionTree.add(new OpAssign(assignmentVarGenerator.getNew(), opToAssign));
        Op forLoopOp = new OpFor(forLoopVarGenerator.getNew(), com.aql.algebra.expressions.Var.alloc(assignmentVarGenerator.getCurrent()));
        //update bound vars
        RewritingUtils.UpdateBoundVariablesMapping(boundVars, forLoopVarGenerator.getCurrent());
        return forLoopOp;
    }

    private Op AddNewAssignmentAndLoop(com.aql.algebra.expressions.Expr exprToAssign, Map<String, String> boundVars){
        //create for loop over query that already had projection by using let stmt
        //Add let stmt to our main query structure
        _aqlAlgebraQueryExpressionTree.add(new OpAssign(assignmentVarGenerator.getNew(), exprToAssign));
        Op forLoopOp = new OpFor(forLoopVarGenerator.getNew(), com.aql.algebra.expressions.Var.alloc(assignmentVarGenerator.getCurrent()));
        //update bound vars
        RewritingUtils.UpdateBoundVariablesMapping(boundVars, forLoopVarGenerator.getCurrent());
        return forLoopOp;
    }

}

