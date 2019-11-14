package com.sparql_to_aql;

import com.aql.algebra.AqlQueryNode;
import com.aql.algebra.expressions.Constant;
import com.aql.algebra.expressions.ExprVar;
import com.aql.algebra.expressions.constants.Const_Object;
import com.aql.algebra.operators.*;
import com.aql.algebra.operators.OpSequence;
import com.aql.algebra.resources.AssignedResource;
import com.aql.algebra.resources.IterationResource;
import com.sparql_to_aql.constants.ArangoAttributes;
import com.sparql_to_aql.constants.ArangoDataModel;
import com.sparql_to_aql.entities.BoundAqlVarDetails;
import com.sparql_to_aql.entities.BoundAqlVars;
import com.sparql_to_aql.entities.algebra.OpDistinctProject;
import com.aql.algebra.expressions.ExprList;
import com.aql.algebra.expressions.constants.Const_Array;
import com.aql.algebra.expressions.constants.Const_Number;
import com.aql.algebra.expressions.constants.Const_String;
import com.aql.algebra.expressions.functions.*;
import com.sparql_to_aql.utils.AqlUtils;
import com.sparql_to_aql.utils.MapUtils;
import com.sparql_to_aql.utils.RewritingUtils;
import com.sparql_to_aql.utils.VariableGenerator;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.query.SortCondition;
import org.apache.jena.sparql.algebra.Table;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.Expr;
import java.util.*;

//TODO cater for empty graph patterns (table unit) in each operator.. they should technically be ignored
// EVEN BETTER would be to create a transformer called EmptyGraphPatternTransformer to remove all unneccesary empty patterns..?
//class used for rewriting of SPARQL algebra query expression to AQL algebra query expression
public abstract class ArqToAqlAlgebraVisitor extends RewritingOpVisitorBase {

    //Aql query can be made of a sequence of "subqueries" and assignments, hence OpSequence
    protected OpSequence _aqlAlgebraQueryExpressionTree;

    ArangoDataModel dataModel;
    List<String> defaultGraphNames;
    List<String> namedGraphNames;

    //This method is to be called after the visitor has been used
    public AqlQueryNode GetAqlAlgebraQueryExpression()
    {
        _aqlAlgebraQueryExpressionTree.add(createdAqlNodes.getFirst());

        return _aqlAlgebraQueryExpressionTree;
    }

    protected VariableGenerator forLoopVarGenerator = new VariableGenerator("forloop", "item");
    protected VariableGenerator assignmentVarGenerator = new VariableGenerator("assign", "item");
    protected VariableGenerator graphForLoopVertexVarGenerator = new VariableGenerator("g_v");
    protected VariableGenerator graphForLoopEdgeVarGenerator = new VariableGenerator("g_e");
    protected VariableGenerator graphForLoopPathVarGenerator = new VariableGenerator("g_p");

    //Keep track of which variables have already been bound (or not, if optional), by mapping ARQ algebra op hashcode to the list of vars
    //the second map is used to map the sparql variable name to the corresponding aql variable name(s) to use (due to for loop variable names)
    protected Map<Integer, Map<String, BoundAqlVars>> boundSparqlVariablesByOp = new HashMap<>();

    //use linked list - easier to pop out and push items from front or back
    protected LinkedList<AqlQueryNode> createdAqlNodes = new LinkedList<>();

    public ArqToAqlAlgebraVisitor(List<String> defaultGraphNames, List<String> namedGraphs, ArangoDataModel dataModel){
        this.defaultGraphNames = defaultGraphNames;
        this.namedGraphNames = namedGraphs;
        this._aqlAlgebraQueryExpressionTree = new OpSequence();
        this.dataModel = dataModel;
    }

    @Override
    public void visit(OpSlice opSlice){
        AqlQueryNode currOp = createdAqlNodes.removeLast();
        long start = opSlice.getStart();
        if(opSlice.getStart() < 0)
            start = 0;

        boolean projectAfterSlice = false;
        Map<String, BoundAqlVars> boundVars = GetSparqlVariablesByOp(opSlice.getSubOp());

        //check if currOp is project, if so add a new for loop with the slicing + project
        if(currOp instanceof com.aql.algebra.operators.OpProject) {
            currOp = AddNewAssignmentAndLoop((Op) currOp, boundVars);
            projectAfterSlice = true;
        }

        Op limitOp = new OpLimit(currOp, start, opSlice.getLength());

        if(projectAfterSlice)
            limitOp = new com.aql.algebra.operators.OpProject(limitOp, new ExprVar(forLoopVarGenerator.getCurrent()), false);

        createdAqlNodes.add(limitOp);
        SetSparqlVariablesByOp(opSlice, boundVars);
    }

    @Override
    public void visit(OpOrder opOrder) {
        AqlQueryNode orderSubOp = createdAqlNodes.removeLast();

        List<SortCondition> sortConditionList = opOrder.getConditions();
        List<com.aql.algebra.SortCondition> aqlSortConds = new ArrayList<>();

        Map<String, BoundAqlVars> boundVars = GetSparqlVariablesByOp(opOrder.getSubOp());

        for (int i= 0; i < sortConditionList.size(); i++) {
            SortCondition currCond = sortConditionList.get(i);

            com.aql.algebra.SortCondition.Direction direction;
            switch (currCond.getDirection()){
                case Query.ORDER_ASCENDING:
                    direction = com.aql.algebra.SortCondition.Direction.ASC;
                    break;
                case Query.ORDER_DESCENDING:
                    direction = com.aql.algebra.SortCondition.Direction.DESC;
                    break;
                default:
                    direction = com.aql.algebra.SortCondition.Direction.DEFAULT;
            }

            com.aql.algebra.expressions.Expr aqlSortExpr = RewritingUtils.ProcessExpr(currCond.getExpression(), boundVars);
            //add .value over sort variable if it is a bound var, since we want the actual value to be sorted (_id, _key, _rev, type properties will otherwise change the sort order)
            if(aqlSortExpr instanceof com.aql.algebra.expressions.ExprVar && boundVars.values().contains(aqlSortExpr.getVarName()))
                aqlSortExpr = new com.aql.algebra.expressions.ExprVar(AqlUtils.buildVar(aqlSortExpr.getVarName(), ArangoAttributes.VALUE));

            aqlSortConds.add(new com.aql.algebra.SortCondition(aqlSortExpr, direction));
        }

        OpSort aqlSort = new OpSort(orderSubOp, aqlSortConds);
        createdAqlNodes.add(aqlSort);
        SetSparqlVariablesByOp(opOrder, boundVars);
    }

    @Override
    public void visit(OpProject opProject){
        boolean useDistinct = false;

        AqlQueryNode currOp = createdAqlNodes.removeLast();
        List<Var> projectableVars = opProject.getVars();
        Map<String, BoundAqlVars> boundVars = GetSparqlVariablesByOp(opProject.getSubOp());

        currOp = EnsureIterationResource(currOp, boundVars);

        if(opProject instanceof OpDistinctProject){
            useDistinct = true;
            /*if(projectableVars.size() == 1){

            }
            else{
                //SELECT DISTINCT WITH >1 VAR = COLLECT in AQL... consider mentioning this in thesis writeup in AQL algebra

                //apply collect stmt over current projectionSubOp
                currOp = new OpCollect(currOp, RewritingUtils.CreateCollectVarExprList(projectableVars, boundVars), null);
            }*/
        }

        com.aql.algebra.expressions.VarExprList returnVariables = RewritingUtils.CreateProjectionVarExprList(projectableVars, boundVars);

        Op returnStmt = new com.aql.algebra.operators.OpProject(currOp, returnVariables, useDistinct);

        //since we have projected all the variables that are required and we're projecting them with the name of the sparql variable already,
        //the mapped AQL variable name is the same as the SPARQL variable name, thus update accordingly
        SetSparqlVariablesByOp(opProject, CreateBoundVarsMap(projectableVars));

        createdAqlNodes.add(returnStmt);
    }

    @Override
    public void visit(OpJoin opJoin){
        if(opJoin.getLeft() instanceof OpTable && opJoin.getRight() instanceof OpTable){
            throw new UnsupportedOperationException("Joins between two VALUES tables are not supported");
        }

        AqlQueryNode opToJoinRight = createdAqlNodes.removeLast();
        AqlQueryNode opToJoinLeft = createdAqlNodes.removeLast();
        Map<String, BoundAqlVars> boundVariablesInOpLeftToJoin = GetSparqlVariablesByOp(opJoin.getLeft());
        Map<String, BoundAqlVars> boundVariablesInOpRightToJoin = GetSparqlVariablesByOp(opJoin.getRight());
        boolean nestLeftWithinRight = false;

        //if the VALUES table is on the left side, we nest it within the right side, ie. forloop for VALUES table is ALWAYS the one that's nested - for performance sake
        if(opJoin.getLeft() instanceof OpTable) {
            nestLeftWithinRight = true;
        }

        //since joining involves adding filter conditions to match the results of both operators
        //we need two for loops that we can nest.
        //We can't nest a projection op so assign it's projected data to a variable and add a new forloop over that data
        opToJoinLeft = EnsureIterationResource(opToJoinLeft, boundVariablesInOpLeftToJoin);
        opToJoinRight = EnsureIterationResource(opToJoinRight, boundVariablesInOpRightToJoin);

        //use list of common variables between the graph patterns that must be joined to add the joining filter conditions
        ExprList filterExprs = GetFiltersOnCommonVars(boundVariablesInOpLeftToJoin, boundVariablesInOpRightToJoin);

        //add used vars in both the graph patterns being joined to sparqlVariablesByOp
        //since by joining we will now have all the variables in this scope
        AddSparqlVariablesByOp(opJoin, boundVariablesInOpLeftToJoin);
        AddSparqlVariablesByOp(opJoin, boundVariablesInOpRightToJoin);

        //nest one for loop in the other and add filter statements
        if(filterExprs.size() > 0) {
            if(nestLeftWithinRight){
                opToJoinLeft = new com.aql.algebra.operators.OpFilter(filterExprs, opToJoinLeft);
            }
            else{
                opToJoinRight = new com.aql.algebra.operators.OpFilter(filterExprs, opToJoinRight);
            }
        }

        if(nestLeftWithinRight) {
            opToJoinLeft = new OpNest(opToJoinRight, opToJoinLeft);
        }
        else{
            opToJoinLeft = new OpNest(opToJoinLeft, opToJoinRight);
        }

        createdAqlNodes.add(opToJoinLeft);
    }

    //TODO cater for FILTER EXISTS and FILTER NOT EXISTS???
    // but how to process nested BGPs like this?! in this case the filter expr wouldn't be just an appended AQL filter but rather we'd need to add a nested subquery for the exists/not exists bgp and add a filter over it..
    // orrr FILTER LENGTH((NESTED FOR LOOP HERE WITH PROJECT) > 0) (or == 0 if not exists)
    @Override
    public void visit(OpFilter opFilter){
        //add aql filter operator over current op
        AqlQueryNode currOp = createdAqlNodes.removeLast();
        Map<String, BoundAqlVars> boundVars = GetSparqlVariablesByOp(opFilter.getSubOp());
        //iterate over expressions, add AQL filter conditions to list
        ExprList filterConds = new ExprList();
        for(Iterator<Expr> i = opFilter.getExprs().iterator(); i.hasNext();){
            filterConds.add(RewritingUtils.ProcessExpr(i.next(), boundVars));
        }

        createdAqlNodes.add(new com.aql.algebra.operators.OpFilter(filterConds, currOp));
        SetSparqlVariablesByOp(opFilter, boundVars);
    }

    @Override
    public void visit(OpLeftJoin opLeftJoin){
        /*Since AQL doesn't directly support left joins, we have to "simulate" it like in the below example, taken from https://www.arangodb.com/why-arangodb/sql-aql-comparison/ (where friend can be null):
        FOR user IN users
          LET friends = (
            FOR friend IN friends
              FILTER friend.user == user._key
              RETURN friend
          )
          FOR friendToJoin IN (
            LENGTH(friends) > 0 ? friends :
              [ {} ]
            )
            RETURN {
                user: user,
                friend: friendToJoin
            }
        */
        AqlQueryNode rightOp = createdAqlNodes.removeLast();
        Map<String, BoundAqlVars> rightBoundVars = GetSparqlVariablesByOp(opLeftJoin.getRight());

        //update all right variables to be canBeNull=true
        MapUtils.UpdateBoundAqlVarsInMap_CanBeNull(rightBoundVars);

        /*if(opLeftJoin.getLeft() instanceof OpTable && ((OpTable) opLeftJoin.getLeft()).isJoinIdentity()) {
            //an optional over an empty group pattern is equivalent to not having an optional at all, ie. just keep all results on right side
            //https://www.slideshare.net/juliandolby/optionals-25419705
            createdAqlNodes.add(rightOp);
            //add bound vars to map
            SetSparqlVariablesByOp(opLeftJoin, rightBoundVars);
            return;
        }*/

        AqlQueryNode leftOp = createdAqlNodes.removeLast();
        Map<String, BoundAqlVars> leftBoundVars = GetSparqlVariablesByOp(opLeftJoin.getLeft());

        leftOp = EnsureIterationResource(leftOp, leftBoundVars);
        String outerLoopVarName = ((IterationResource) leftOp).getIterationVar().getVarName();

        //add filters on the right side results to make sure common variables match to those on the left
        ExprList filtersExprs = GetFiltersOnCommonVars(leftBoundVars, rightBoundVars);

        //if left join contains exprs, apply filter exprs on optional (right) part since that will be nested inside the left part
        if(opLeftJoin.getExprs() != null) {
            for (Iterator<Expr> i = opLeftJoin.getExprs().iterator(); i.hasNext(); ) {
                filtersExprs.add(RewritingUtils.ProcessExpr(i.next(), rightBoundVars));
            }
        }

        if(!(rightOp instanceof com.aql.algebra.operators.OpProject)){
            if(filtersExprs.size() > 0)
                rightOp = new com.aql.algebra.operators.OpFilter(filtersExprs, rightOp);
            //add project over right op
            rightOp = new com.aql.algebra.operators.OpProject(rightOp, RewritingUtils.CreateProjectionVarExprList(rightBoundVars), false);
        }
        else{
            //add filter stmts within the project stmt to avoid an extra assignment
            com.aql.algebra.operators.OpProject rightProjectOp = (com.aql.algebra.operators.OpProject) rightOp;
            rightOp = new com.aql.algebra.operators.OpProject(new com.aql.algebra.operators.OpFilter(filtersExprs, rightProjectOp.getChild()), rightProjectOp.getExprs(), false);
        }

        //we want to find the solutions on the right side that are compatible with the ones on the left side
        //so we nest the right Op within the left Op, and add filters on that nested right Op to only keep the solutions that match the current solution on the left side,
        //and store those solutions into a variable
        AssignedResource innerAssignment = new AssignedResource(assignmentVarGenerator.getNew(), (Op)rightOp);

        AqlQueryNode newOp = new com.aql.algebra.operators.OpNest(leftOp, innerAssignment);

        // here we will loop over the matching solutions found in the right Op,
        // such that if no matching solutions were found, we simply project the data on the left side,
        // and if there were matching solutions, we project the join of the left and right data
        AqlQueryNode subquery = new IterationResource(forLoopVarGenerator.getNew(), new Expr_Conditional(new Expr_GreaterThan(new Expr_Length(new ExprVar(assignmentVarGenerator.getCurrent())), new Const_Number(0)), new ExprVar(assignmentVarGenerator.getCurrent()), new Const_Array(new Const_Object())));
        newOp = new OpNest(newOp, subquery);

        //TODO make sure that MERGE works well if there are common attributes between the parameters.. https://www.arangodb.com/docs/stable/aql/functions-document.html#merge
        // especially due to UNDEFs/Optional vars...
        newOp = new com.aql.algebra.operators.OpProject(newOp, new Expr_Merge(new ExprVar(outerLoopVarName), new ExprVar(forLoopVarGenerator.getCurrent())),false);

        Map<String, BoundAqlVars> boundVars = MapUtils.MergeBoundAqlVarsMaps(leftBoundVars, rightBoundVars);
        newOp = AddNewAssignmentAndLoop((Op)newOp, boundVars);
        createdAqlNodes.add(newOp);
        //add bound vars to map
        SetSparqlVariablesByOp(opLeftJoin, boundVars);
    }

    @Override
    public void visit(OpUnion opUnion){
        //how we perform this operation depends if the union is between subqueries that have a projection or not
        //example of what we need: LET unionResult = UNION(left_result_here, right_result_here);
        AqlQueryNode rightOp = createdAqlNodes.removeLast();
        AqlQueryNode leftOp = createdAqlNodes.removeLast();

        Map<String, BoundAqlVars> leftBoundVars = GetSparqlVariablesByOp(opUnion.getLeft());
        Map<String, BoundAqlVars> rightBoundVars = GetSparqlVariablesByOp(opUnion.getRight());

        //we need to assign two variables - one holding the result of the query of the leftOp, another with that of the right
        //hence why we need to make sure that we have a projection over each of them
        leftOp = EnsureProjection(leftOp, leftBoundVars);
        rightOp = EnsureProjection(rightOp, rightBoundVars);

        AddNewAssignment((Op)leftOp);
        String leftAssignVar = assignmentVarGenerator.getCurrent();
        AddNewAssignment((Op)rightOp);
        String rightAssignVar = assignmentVarGenerator.getCurrent();

        Map<String, BoundAqlVars> allBoundVars = MapUtils.MergeBoundAqlVarsMaps(leftBoundVars, rightBoundVars);
        //union the results of both sides and assign the result to another variable + add a new loop over this latest variable
        //as we always need to have at least one node in createdAqlNodes
        createdAqlNodes.add(AddNewAssignmentAndLoop(new Expr_Union(new ExprVar(leftAssignVar), new ExprVar(rightAssignVar)), allBoundVars));
        AddSparqlVariablesByOp(opUnion, allBoundVars);
    }

    @Override
    public void visit(OpMinus opMinus){
        // we need a for loop within a for loop with a filter to find all solution mappings in rightOp that are compatible solution mappings from leftOp
        // and we only keep mappings from leftOp were the count() of items returned by that inner for loop is 0
        // Example:
        // FOR l in leftOp
        //      Let c = FOR r IN rightOp
        //         FILTER (l.v1 = r.v1) AND (l.v2 = r.v2)
        //         COLLECT WITH COUNT INTO length
        //         RETURN length
        //      FILTER c == 0

        AqlQueryNode rightOp = createdAqlNodes.removeLast();
        AqlQueryNode leftOp = createdAqlNodes.removeLast();

        Map<String, BoundAqlVars> leftBoundVars = GetSparqlVariablesByOp(opMinus.getLeft());
        Map<String, BoundAqlVars> rightBoundVars = GetSparqlVariablesByOp(opMinus.getRight());

        Set<String> commonVars = MapUtils.GetCommonMapKeys(leftBoundVars, rightBoundVars);

        //the MINUS operator only affects the result if there are common variables between both graph patterns
        //otherwise all the data on the left side is retained
        if(commonVars.size() == 0){
            createdAqlNodes.add(leftOp);
            AddSparqlVariablesByOp(opMinus, leftBoundVars);
            return;
        }

        //a forloop with filters etc. attached is still fine, so as long as op isn't a project, do nothing, else add that iteration resource
        leftOp = EnsureIterationResource(leftOp, leftBoundVars, true);
        rightOp = EnsureIterationResource(rightOp, rightBoundVars, true);

        ExprList filtersExprs = GetFiltersOnCommonVars(leftBoundVars, rightBoundVars);

        rightOp = new com.aql.algebra.operators.OpFilter(filtersExprs, rightOp);
        ExprVar countVar = new ExprVar("length");
        rightOp = new OpCollect(rightOp, countVar);
        rightOp = new com.aql.algebra.operators.OpProject(rightOp, countVar, false);

        //TODO if we start allowing subqueries (AqlQueryNode) in expressions, we wouldn't need an extra assignment here
        leftOp = new OpNest(leftOp, new AssignedResource(assignmentVarGenerator.getNew(), (Op)rightOp));
        leftOp = new com.aql.algebra.operators.OpFilter(new Expr_Equals(new ExprVar(assignmentVarGenerator.getCurrent()), new Const_Number(0)), leftOp);
        createdAqlNodes.add(leftOp);
        AddSparqlVariablesByOp(opMinus, leftBoundVars);
    }

    @Override
    public void visit(OpExtend opExtend){
        //we need to use LET assignments to bind the variables to computed values
        AqlQueryNode currOp = createdAqlNodes.removeLast();

        VarExprList varExprList = opExtend.getVarExprList();

        Map<String, BoundAqlVars> prevBoundVars = GetSparqlVariablesByOp(opExtend.getSubOp());

        //nest an OpSequence instead of adding lots of nests
        OpSequence assignments = new OpSequence();
        for(Var v: varExprList.getVars()){
            assignments.add(new AssignedResource(v.getVarName(), RewritingUtils.ProcessExpr(varExprList.getExpr(v), prevBoundVars)));
        }

        currOp = new com.aql.algebra.operators.OpNest(currOp, assignments);

        createdAqlNodes.add(currOp);

        AddSparqlVariablesByOp(opExtend, CreateBoundVarsMap(varExprList.getVars()));
        AddSparqlVariablesByOp(opExtend, prevBoundVars);
    }

    @Override
    public void visit(OpTable opTable){

        //an empty graph pattern doesn't affect our result, so ignore it
        //we will check in
        //if(opTable.isJoinIdentity())
            //return;

        //process table row by row
        Table solutionSequences = opTable.getTable();
        List<Var> vars = solutionSequences.getVars();
        List<Constant> listOfAqlObjects = new ArrayList<>();

        Map<String, BoundAqlVars> boundVars = CreateBoundVarsMap(vars);

        //create a JSON object for each possible solution sequence
        for (Iterator<Binding> i = solutionSequences.rows(); i.hasNext();){
            Map<String, com.aql.algebra.expressions.Expr> objectProperties = new HashMap<>();
            Binding b = i.next();
            for(Var var : vars){
                Node value = b.get(var);

                //if a value for a variable is UNDEF, then in the boundVars map set that variable to canBeNull = true
                if(value == null) {
                    boundVars.get(var.getVarName()).setAllCanBeNull();
                    continue;
                }

                objectProperties.put(var.getVarName(), RewritingUtils.ValuesRdfNodeToArangoObject(value));
            }

            listOfAqlObjects.add(new Const_Object(objectProperties));
        }

        //TODO instead of assigning the table values to a seperate variable, consider just putting it directly in the forloop..
        Const_Array array = new Const_Array(listOfAqlObjects.toArray(new Constant[listOfAqlObjects.size()]));
        AqlQueryNode forLoop = AddNewAssignmentAndLoop(array, boundVars);
        createdAqlNodes.add(forLoop);
        AddSparqlVariablesByOp(opTable, boundVars);
    }

    /**
     * Add map of bound SPARQL to AQL variables in the scope of a particular SPARQL operator
     * @param op SPARQL operator
     * @param variables map of bound SPARQL to AQL variables
     */
    protected void AddSparqlVariablesByOp(org.apache.jena.sparql.algebra.Op op, Map<String, BoundAqlVars> variables){
        Map<String, BoundAqlVars> currUsedVars = GetSparqlVariablesByOp(op);
        if(currUsedVars == null || currUsedVars.size() == 0) {
            boundSparqlVariablesByOp.put(op.hashCode(), variables);
        }
        else {
            MapUtils.MergeBoundAqlVarsMaps(currUsedVars, variables);
        }
    }

    protected void SetSparqlVariablesByOp(org.apache.jena.sparql.algebra.Op op, Map<String, BoundAqlVars> variables){
        boundSparqlVariablesByOp.put(op.hashCode(), variables);
    }

    protected Map<String, BoundAqlVars> GetSparqlVariablesByOp(org.apache.jena.sparql.algebra.Op op){
        Map<String, BoundAqlVars> currUsedVars = boundSparqlVariablesByOp.get(op.hashCode());
        if(currUsedVars == null)
            return new HashMap<>();

        return currUsedVars;
    }

    /**
     * This method takes a list of graphs specified in FROM clauses OR a list of graphs specified in FROM NAMED clauses
     * and adds filters on the current for loop to make sure only triples in these graphs are considered
     * @param graphNames list of graph uris
     * @param forLoopVarName AQL variable name of current forloop
     * @param filterConditions current list of filter conditions in the for loop
     */
    protected void AddGraphFilters(List<String> graphNames, String forLoopVarName, ExprList filterConditions){
        com.aql.algebra.expressions.Expr filterExpr = null;

        //add filters for default or named graphs
        for(String g: graphNames){
            com.aql.algebra.expressions.Expr currExpr = new Expr_Equals(new ExprVar(AqlUtils.buildVar(forLoopVarName, ArangoAttributes.GRAPH_NAME, ArangoAttributes.VALUE)), new Const_String(g));

            if(filterExpr == null){
                filterExpr = currExpr;
            }
            else{
                filterExpr = new Expr_LogicalOr(filterExpr, currExpr);
            }
        }

        filterConditions.add(filterExpr);
    }

    protected void AddNewAssignment(Op opToAssign){
        _aqlAlgebraQueryExpressionTree.add(new AssignedResource(assignmentVarGenerator.getNew(), opToAssign));
    }

    protected void AddNewAssignment(com.aql.algebra.expressions.Expr exprToAssign){
        _aqlAlgebraQueryExpressionTree.add(new AssignedResource(assignmentVarGenerator.getNew(), exprToAssign));
    }

    protected AqlQueryNode AddNewAssignmentAndLoop(Op opToAssign, Map<String, BoundAqlVars> boundVars){
        //create for loop over query that already had projection by using let stmt
        //Add let stmt to our main query structure
        _aqlAlgebraQueryExpressionTree.add(new AssignedResource(assignmentVarGenerator.getNew(), opToAssign));
        AqlQueryNode forLoop = new IterationResource(forLoopVarGenerator.getNew(), new ExprVar(assignmentVarGenerator.getCurrent()));
        //update bound vars
        RewritingUtils.UpdateBoundVariablesMapping(boundVars, forLoopVarGenerator.getCurrent());
        return forLoop;
    }

    protected AqlQueryNode AddNewAssignmentAndLoop(com.aql.algebra.expressions.Expr exprToAssign, Map<String, BoundAqlVars> boundVars){
        //create for loop over query that already had projection by using let stmt
        //Add let stmt to our main query structure
        _aqlAlgebraQueryExpressionTree.add(new AssignedResource(assignmentVarGenerator.getNew(), exprToAssign));
        AqlQueryNode forLoop = new IterationResource(forLoopVarGenerator.getNew(), new ExprVar(assignmentVarGenerator.getCurrent()));
        //update bound vars
        RewritingUtils.UpdateBoundVariablesMapping(boundVars, forLoopVarGenerator.getCurrent());
        return forLoop;
    }

    protected AqlQueryNode EnsureIterationResource(AqlQueryNode node, Map<String, BoundAqlVars> boundVars){
        return EnsureIterationResource(node, boundVars, false);
    }

    protected AqlQueryNode EnsureIterationResource(AqlQueryNode node, Map<String, BoundAqlVars> boundVars, boolean onlyIfCurrOpIsProject){
        if(!(node instanceof IterationResource)) {
            if(!(node instanceof com.aql.algebra.operators.OpProject)){
                if(onlyIfCurrOpIsProject)
                    return node;

                //add project over left op + let stmt and then create for loop which we need
                node = new com.aql.algebra.operators.OpProject(node, RewritingUtils.CreateProjectionVarExprList(boundVars), false);
            }
            node = AddNewAssignmentAndLoop((Op)node, boundVars);
        }

        return node;
    }

    protected AqlQueryNode EnsureProjection(AqlQueryNode node, Map<String, BoundAqlVars> boundVars){
        if(!(node instanceof com.aql.algebra.operators.OpProject)){
            node = new com.aql.algebra.operators.OpProject(node, RewritingUtils.CreateProjectionVarExprList(boundVars), false);
        }

        return node;
    }

    /**
     * Create a map of bound SPARQL variables to AQL variables with the same name
     * @param vars list of SPARQL variables
     * @return map of bound variables
     */
    protected Map<String, BoundAqlVars> CreateBoundVarsMap(List<Var> vars){
        Map<String, BoundAqlVars> boundVars = new HashMap<>();
        vars.forEach(v -> boundVars.put(v.getName(), new BoundAqlVars(v.getName())));
        return boundVars;
    }

    protected com.aql.algebra.expressions.ExprList GetFiltersOnCommonVars(Map<String, BoundAqlVars> leftBoundVars, Map<String, BoundAqlVars> rightBoundVars){
        return RewritingUtils.GetFiltersOnCommonVars(leftBoundVars, rightBoundVars, dataModel);
    }

}

