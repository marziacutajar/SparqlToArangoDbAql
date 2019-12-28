package com.sparql_to_aql;

import com.aql.querytree.AqlQueryNode;
import com.aql.querytree.expressions.Constant;
import com.aql.querytree.expressions.ExprVar;
import com.aql.querytree.expressions.constants.Const_Object;
import com.aql.querytree.operators.*;
import com.aql.querytree.operators.OpSequence;
import com.aql.querytree.resources.AssignedResource;
import com.aql.querytree.resources.IterationResource;
import com.sparql_to_aql.constants.ArangoAttributes;
import com.sparql_to_aql.constants.ArangoDataModel;
import com.sparql_to_aql.entities.BoundAqlVars;
import com.sparql_to_aql.entities.BoundSparqlVariablesByOp;
import com.sparql_to_aql.entities.algebra.OpDistinctProject;
import com.aql.querytree.expressions.ExprList;
import com.aql.querytree.expressions.constants.Const_Array;
import com.aql.querytree.expressions.constants.Const_Number;
import com.aql.querytree.expressions.constants.Const_String;
import com.aql.querytree.expressions.functions.*;
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

//class used for rewriting of SPARQL algebra tree  to AQL query tree
public abstract class ArqToAqlTreeVisitor extends RewritingOpVisitorBase {

    //Aql query can be made of a sequence of "subqueries" and assignments, hence OpSequence
    protected OpSequence _aqlQueryExpressionTree;

    ArangoDataModel dataModel;
    List<String> defaultGraphNames;
    List<String> namedGraphNames;

    //This method is to be called after the visitor has been used
    public OpSequence GetAqlQueryTree()
    {
        _aqlQueryExpressionTree.add(createdAqlNodes.getFirst());

        return _aqlQueryExpressionTree;
    }

    public Map<String, BoundAqlVars> GetLastBoundVars()
    {
        return boundSparqlVariablesByOp.getLastBoundVars();
    }

    protected VariableGenerator forLoopVarGenerator = new VariableGenerator("forloop", "item");
    protected VariableGenerator assignmentVarGenerator = new VariableGenerator("assign", "item");
    protected VariableGenerator graphForLoopVertexVarGenerator = new VariableGenerator("g_v");
    protected VariableGenerator graphForLoopEdgeVarGenerator = new VariableGenerator("g_e");
    protected VariableGenerator graphForLoopPathVarGenerator = new VariableGenerator("g_p");

    //TODO consider not storing these by Op, but rather keep an ordered list and we always get them from the end (like with createdAqlNodes)
    // it would make it simpler to pass boundVars to process FILTER (NOT) EXISTS expressions too
    protected BoundSparqlVariablesByOp boundSparqlVariablesByOp = new BoundSparqlVariablesByOp();

    //use linked list - easier to pop out and push items from front or back
    protected LinkedList<AqlQueryNode> createdAqlNodes = new LinkedList<>();

    public ArqToAqlTreeVisitor(List<String> defaultGraphNames, List<String> namedGraphs, ArangoDataModel dataModel){
        this.defaultGraphNames = defaultGraphNames == null ? new ArrayList<>() : defaultGraphNames;
        this.namedGraphNames = namedGraphs == null ? new ArrayList<>() : namedGraphs;
        this._aqlQueryExpressionTree = new OpSequence();
        this.dataModel = dataModel;
    }

    public ArqToAqlTreeVisitor(List<String> defaultGraphNames, List<String> namedGraphs, ArangoDataModel dataModel,
                               VariableGenerator forLoopVarGen, VariableGenerator assignmentVarGen, VariableGenerator graphVertexVarGen, VariableGenerator graphEdgeVarGen, VariableGenerator graphPathVarGen){
        this.defaultGraphNames = defaultGraphNames == null ? new ArrayList<>() : defaultGraphNames;
        this.namedGraphNames = namedGraphs == null ? new ArrayList<>() : namedGraphs;
        this._aqlQueryExpressionTree = new OpSequence();
        this.dataModel = dataModel;
        this.forLoopVarGenerator = forLoopVarGen;
        this.assignmentVarGenerator = assignmentVarGen;
        this.graphForLoopEdgeVarGenerator = graphEdgeVarGen;
        this.graphForLoopPathVarGenerator = graphPathVarGen;
        this.graphForLoopVertexVarGenerator = graphVertexVarGen;
    }

    @Override
    public void visit(OpSlice opSlice){
        AqlQueryNode currOp = createdAqlNodes.removeLast();
        long start = opSlice.getStart();
        if(opSlice.getStart() < 0)
            start = 0;

        boolean projectAfterSlice = false;
        Map<String, BoundAqlVars> boundVars = boundSparqlVariablesByOp.getSparqlVariablesByOp(opSlice.getSubOp());

        //check if currOp is project, if so add a new for loop with the slicing + project
        if(currOp instanceof com.aql.querytree.operators.OpProject) {
            currOp = AddNewAssignmentAndLoop((Op) currOp, boundVars);
            projectAfterSlice = true;
        }

        Op limitOp = new OpLimit(currOp, start, opSlice.getLength());

        if(projectAfterSlice)
            limitOp = new com.aql.querytree.operators.OpProject(limitOp, new ExprVar(forLoopVarGenerator.getCurrent()), false);

        createdAqlNodes.add(limitOp);
        boundSparqlVariablesByOp.setSparqlVariablesByOp(opSlice, boundVars);
    }

    @Override
    public void visit(OpOrder opOrder) {
        AqlQueryNode orderSubOp = createdAqlNodes.removeLast();

        List<SortCondition> sortConditionList = opOrder.getConditions();
        List<com.aql.querytree.SortCondition> aqlSortConds = new ArrayList<>();

        Map<String, BoundAqlVars> boundVars = boundSparqlVariablesByOp.getSparqlVariablesByOp(opOrder.getSubOp());

        for (int i= 0; i < sortConditionList.size(); i++) {
            SortCondition currCond = sortConditionList.get(i);

            com.aql.querytree.SortCondition.Direction direction;
            switch (currCond.getDirection()){
                case Query.ORDER_ASCENDING:
                    direction = com.aql.querytree.SortCondition.Direction.ASC;
                    break;
                case Query.ORDER_DESCENDING:
                    direction = com.aql.querytree.SortCondition.Direction.DESC;
                    break;
                default:
                    direction = com.aql.querytree.SortCondition.Direction.DEFAULT;
            }

            com.aql.querytree.expressions.Expr aqlSortExpr = RewritingUtils.ProcessExpr(currCond.getExpression(), boundVars, dataModel, forLoopVarGenerator, assignmentVarGenerator, graphForLoopVertexVarGenerator, graphForLoopEdgeVarGenerator, graphForLoopPathVarGenerator);

            if(dataModel == ArangoDataModel.G && aqlSortExpr instanceof com.aql.querytree.expressions.ExprVar){
                //add .value over sort variable, since we want the actual value to be sorted (_id, _key, _rev properties will otherwise change the sort order)
                //also do the same to sort by type
                String varName = aqlSortExpr.getVarName();
                aqlSortExpr = new com.aql.querytree.expressions.ExprVar(AqlUtils.buildVar(varName, ArangoAttributes.TYPE));
                aqlSortConds.add(new com.aql.querytree.SortCondition(aqlSortExpr, direction));
                aqlSortExpr = new com.aql.querytree.expressions.ExprVar(AqlUtils.buildVar(varName, ArangoAttributes.VALUE));
                aqlSortConds.add(new com.aql.querytree.SortCondition(aqlSortExpr, direction));
            }
            else{
                aqlSortConds.add(new com.aql.querytree.SortCondition(aqlSortExpr, direction));
            }
        }

        OpSort aqlSort = new OpSort(orderSubOp, aqlSortConds);
        createdAqlNodes.add(aqlSort);
        boundSparqlVariablesByOp.setSparqlVariablesByOp(opOrder, boundVars);
    }

    @Override
    public void visit(OpProject opProject){
        boolean useDistinct = false;

        AqlQueryNode currOp = createdAqlNodes.removeLast();
        List<Var> projectableVars = opProject.getVars();
        Map<String, BoundAqlVars> boundVars = boundSparqlVariablesByOp.getSparqlVariablesByOp(opProject.getSubOp());

        currOp = EnsureIteration(currOp, boundVars);

        if(opProject instanceof OpDistinctProject){
            useDistinct = true;
        }

        com.aql.querytree.expressions.VarExprList returnVariables = RewritingUtils.CreateProjectionVarExprList(projectableVars, boundVars);

        Op returnStmt = new com.aql.querytree.operators.OpProject(currOp, returnVariables, useDistinct);

        //since we have projected all the variables that are required and we're projecting them with the name of the sparql variable already,
        //the mapped AQL variable name is the same as the SPARQL variable name, thus update accordingly
        boundSparqlVariablesByOp.setSparqlVariablesByOp(opProject, CreateBoundVarsMap(projectableVars));

        createdAqlNodes.add(returnStmt);
    }

    @Override
    public void visit(OpJoin opJoin){
        AqlQueryNode opToJoinRight = createdAqlNodes.removeLast();
        AqlQueryNode opToJoinLeft = createdAqlNodes.removeLast();
        Map<String, BoundAqlVars> boundVariablesInOpLeftToJoin = boundSparqlVariablesByOp.getSparqlVariablesByOp(opJoin.getLeft());
        Map<String, BoundAqlVars> boundVariablesInOpRightToJoin = boundSparqlVariablesByOp.getSparqlVariablesByOp(opJoin.getRight());
        boolean nestLeftWithinRight = false;

        //if the VALUES table is on the left side, we nest it within the right side, ie. forloop for VALUES table is ALWAYS the one that's nested - for performance sake
        if(opJoin.getLeft() instanceof OpTable) {
            nestLeftWithinRight = true;
        }

        //since joining involves adding filter conditions to match the results of both operators
        //we need two for loops that we can nest.
        //We can't nest a projection op so assign it's projected data to a variable and add a new forloop over that data
        opToJoinLeft = EnsureIteration(opToJoinLeft, boundVariablesInOpLeftToJoin);
        opToJoinRight = EnsureIteration(opToJoinRight, boundVariablesInOpRightToJoin);

        //use list of common variables between the graph patterns that must be joined to add the joining filter conditions
        ExprList filterExprs = GetFiltersOnCommonVars(boundVariablesInOpLeftToJoin, boundVariablesInOpRightToJoin);

        //add used vars in both the graph patterns being joined to sparqlVariablesByOp
        //since by joining we will now have all the variables in this scope
        boundSparqlVariablesByOp.setSparqlVariablesByOp(opJoin, MapUtils.MergeBoundAqlVarsMaps(boundVariablesInOpLeftToJoin, boundVariablesInOpRightToJoin));

        //nest one for loop in the other and add filter statements
        if(filterExprs.size() > 0) {
            if(nestLeftWithinRight){
                opToJoinLeft = new com.aql.querytree.operators.OpFilter(filterExprs, opToJoinLeft);
            }
            else{
                opToJoinRight = new com.aql.querytree.operators.OpFilter(filterExprs, opToJoinRight);
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

    @Override
    public void visit(OpFilter opFilter){
        //add aql filter operator over current op
        AqlQueryNode currOp = createdAqlNodes.removeLast();
        Map<String, BoundAqlVars> boundVars = boundSparqlVariablesByOp.getSparqlVariablesByOp(opFilter.getSubOp());
        //iterate over expressions, add AQL filter conditions to list
        ExprList filterConds = new ExprList();
        for(Iterator<Expr> i = opFilter.getExprs().iterator(); i.hasNext();){
            filterConds.add(RewritingUtils.ProcessExpr(i.next(), boundVars, dataModel, forLoopVarGenerator, assignmentVarGenerator, graphForLoopVertexVarGenerator, graphForLoopEdgeVarGenerator, graphForLoopPathVarGenerator));
        }

        createdAqlNodes.add(new com.aql.querytree.operators.OpFilter(filterConds, currOp));
        boundSparqlVariablesByOp.setSparqlVariablesByOp(opFilter, boundVars);
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
        Map<String, BoundAqlVars> rightBoundVars = boundSparqlVariablesByOp.getSparqlVariablesByOp(opLeftJoin.getRight());

        AqlQueryNode leftOp = createdAqlNodes.removeLast();
        Map<String, BoundAqlVars> leftBoundVars = boundSparqlVariablesByOp.getSparqlVariablesByOp(opLeftJoin.getLeft());

        //I only want an iteration resource here so I don't nest too many FOR loops within each other.. it can be
        //easier to understand if we use LET assignments every now and then
        leftOp = EnsureIterationResource(leftOp, leftBoundVars);

        //add filters on the right side results to make sure common variables match to those on the left
        ExprList filtersExprs = GetFiltersOnCommonVars(leftBoundVars, rightBoundVars);

        //update all right variables that are not DEFINITELY bound by the leftOp to canBeNull=true
        //we do not update these earlier because it would affect the generated join filter conditions above
        for (Map.Entry<String, BoundAqlVars> entry : rightBoundVars.entrySet()) {
            if(leftBoundVars.containsKey(entry.getKey())){
                if(leftBoundVars.get(entry.getKey()).canBeNull())
                    entry.getValue().updateCanBeNull(true);
            }
            else
                entry.getValue().updateCanBeNull(true);
        }

        //if left join contains exprs, apply filter exprs on optional (right) part since that will be nested inside the left part
        if(opLeftJoin.getExprs() != null) {
            for (Iterator<Expr> i = opLeftJoin.getExprs().iterator(); i.hasNext(); ) {
                filtersExprs.add(RewritingUtils.ProcessExpr(i.next(), rightBoundVars, dataModel, forLoopVarGenerator, assignmentVarGenerator, graphForLoopVertexVarGenerator, graphForLoopEdgeVarGenerator, graphForLoopPathVarGenerator));
            }
        }

        if(!(rightOp instanceof com.aql.querytree.operators.OpProject)){
            if(filtersExprs.size() > 0)
                rightOp = new com.aql.querytree.operators.OpFilter(filtersExprs, rightOp);
            //add project over right op
            rightOp = new com.aql.querytree.operators.OpProject(rightOp, RewritingUtils.CreateProjectionVarExprList(rightBoundVars), false);
        }
        else{
            //add filter stmts within the project stmt to avoid an extra assignment
            com.aql.querytree.operators.OpProject rightProjectOp = (com.aql.querytree.operators.OpProject) rightOp;
            rightOp = new com.aql.querytree.operators.OpProject(new com.aql.querytree.operators.OpFilter(filtersExprs, rightProjectOp.getChild()), rightProjectOp.getExprs(), false);
        }

        //we want to find the solutions on the right side that are compatible with the ones on the left side
        //so we nest the right Op within the left Op, and add filters on that nested right Op to only keep the solutions that match the current solution on the left side,
        //and store those solutions into a variable
        AssignedResource innerAssignment = new AssignedResource(assignmentVarGenerator.getNew(), (Op)rightOp);

        AqlQueryNode newOp = new com.aql.querytree.operators.OpNest(leftOp, innerAssignment);

        // here we will loop over the matching solutions found in the right Op,
        // such that if no matching solutions were found, we simply project the data on the left side,
        // and if there were matching solutions, we project the join of the left and right data
        AqlQueryNode subquery = new IterationResource(forLoopVarGenerator.getNew(), new Expr_Conditional(new Expr_GreaterThan(new Expr_Length(new ExprVar(assignmentVarGenerator.getCurrent())), new Const_Number(0)), new ExprVar(assignmentVarGenerator.getCurrent()), new Const_Array(new Const_Object())));
        newOp = new OpNest(newOp, subquery);
        RewritingUtils.UpdateBoundVariablesMapping(rightBoundVars, forLoopVarGenerator.getCurrent(), true);

        Map<String, BoundAqlVars> boundVars = MapUtils.MergeBoundAqlVarsMaps(leftBoundVars, rightBoundVars);
        createdAqlNodes.add(newOp);
        //add bound vars to map
        boundSparqlVariablesByOp.setSparqlVariablesByOp(opLeftJoin, boundVars);
    }

    @Override
    public void visit(OpUnion opUnion){
        //how we perform this operation depends if the union is between subqueries that have a projection or not
        //example of what we need: LET unionResult = UNION(left_result_here, right_result_here);
        AqlQueryNode rightOp = createdAqlNodes.removeLast();
        AqlQueryNode leftOp = createdAqlNodes.removeLast();

        Map<String, BoundAqlVars> leftBoundVars = boundSparqlVariablesByOp.getSparqlVariablesByOp(opUnion.getLeft());
        Map<String, BoundAqlVars> rightBoundVars = boundSparqlVariablesByOp.getSparqlVariablesByOp(opUnion.getRight());

        //we need to assign two variables - one holding the result of the query of the leftOp, another with that of the right
        //hence why we need to make sure that we have a projection over each of them
        leftOp = EnsureProjection(leftOp, leftBoundVars);
        rightOp = EnsureProjection(rightOp, rightBoundVars);

        //commented below since to shorten query, instead of using 2 LETs, we can just nest them in the union as ExprSubquery objects
        AddNewAssignment((Op)leftOp);
        String leftAssignVar = assignmentVarGenerator.getCurrent();
        AddNewAssignment((Op)rightOp);
        String rightAssignVar = assignmentVarGenerator.getCurrent();

        Map<String, BoundAqlVars> allBoundVars = MapUtils.MergeBoundAqlVarsMaps(leftBoundVars, rightBoundVars);
        //union the results of both sides and assign the result to another variable + add a new loop over this latest variable
        //as we always need to have at least one node in createdAqlNodes
        createdAqlNodes.add(AddNewAssignmentAndLoop(new Expr_Union(new ExprVar(leftAssignVar), new ExprVar(rightAssignVar)), allBoundVars));
        //createdAqlNodes.add(AddNewAssignmentAndLoop(new Expr_Union(new ExprSubquery((Op)leftOp), new ExprSubquery((Op)rightOp)), allBoundVars));
        boundSparqlVariablesByOp.setSparqlVariablesByOp(opUnion, allBoundVars);
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

        Map<String, BoundAqlVars> leftBoundVars = boundSparqlVariablesByOp.getSparqlVariablesByOp(opMinus.getLeft());
        Map<String, BoundAqlVars> rightBoundVars = boundSparqlVariablesByOp.getSparqlVariablesByOp(opMinus.getRight());

        Set<String> commonVars = MapUtils.GetCommonMapKeys(leftBoundVars, rightBoundVars);

        //the MINUS operator only affects the result if there are common variables between both graph patterns
        //otherwise all the data on the left side is retained
        if(commonVars.size() == 0){
            createdAqlNodes.add(leftOp);
            boundSparqlVariablesByOp.setSparqlVariablesByOp(opMinus, leftBoundVars);
            return;
        }

        //a forloop with filters etc. attached is still fine, so as long as op isn't a project, do nothing, else add that iteration resource
        leftOp = EnsureIteration(leftOp, leftBoundVars);
        rightOp = EnsureIteration(rightOp, rightBoundVars);

        ExprList filtersExprs = GetFiltersOnCommonVars(leftBoundVars, rightBoundVars);

        rightOp = new com.aql.querytree.operators.OpFilter(filtersExprs, rightOp);
        ExprVar countVar = new ExprVar("length");

        rightOp = new OpCollect(rightOp, countVar);
        rightOp = new com.aql.querytree.operators.OpProject(rightOp, countVar, false);

        //below 2 lines can be replace by the line below them
        //in order to use the ExprSubquery directly in the filter condition and remove the extra assignment
        leftOp = new OpNest(leftOp, new AssignedResource(assignmentVarGenerator.getNew(), (Op)rightOp));
        leftOp = new com.aql.querytree.operators.OpFilter(new Expr_Equals(new ExprVar(assignmentVarGenerator.getCurrent()), new Const_Number(0)), leftOp);
        //leftOp = new com.aql.algebra.operators.OpFilter(new Expr_Equals(new ExprSubquery((Op)rightOp), new Const_Number(0)), leftOp);
        createdAqlNodes.add(leftOp);
        boundSparqlVariablesByOp.setSparqlVariablesByOp(opMinus, leftBoundVars);
    }

    @Override
    public void visit(OpExtend opExtend){
        //we need to use LET assignments to bind the variables to computed values
        AqlQueryNode currOp = createdAqlNodes.removeLast();

        VarExprList varExprList = opExtend.getVarExprList();

        Map<String, BoundAqlVars> prevBoundVars = boundSparqlVariablesByOp.getSparqlVariablesByOp(opExtend.getSubOp());

        //nest an OpSequence instead of adding lots of nests
        OpSequence assignments = new OpSequence();
        for(Var v: varExprList.getVars()){
            assignments.add(new AssignedResource(v.getVarName(), RewritingUtils.ProcessExpr(varExprList.getExpr(v), prevBoundVars, dataModel, forLoopVarGenerator, assignmentVarGenerator, graphForLoopVertexVarGenerator, graphForLoopEdgeVarGenerator, graphForLoopPathVarGenerator)));
        }

        currOp = new com.aql.querytree.operators.OpNest(currOp, assignments);

        createdAqlNodes.add(currOp);

        boundSparqlVariablesByOp.setSparqlVariablesByOp(opExtend, MapUtils.MergeBoundAqlVarsMaps(prevBoundVars, CreateBoundVarsMap(varExprList.getVars())));
    }

    @Override
    public void visit(OpTable opTable){

        //if the table is a Join Identity, we just iterate over an array with one empty object
        //and it doesn't affect the the results in a Join
        if(opTable.isJoinIdentity()){
            createdAqlNodes.add(new IterationResource(forLoopVarGenerator.getNew(), new Const_Array(new Const_Object())));
            boundSparqlVariablesByOp.setSparqlVariablesByOp(opTable, new HashMap<>());
            return;
        }

        //process table row by row
        Table solutionSequences = opTable.getTable();
        List<Var> vars = solutionSequences.getVars();
        List<Constant> listOfAqlObjects = new ArrayList<>();

        Map<String, BoundAqlVars> boundVars = CreateBoundVarsMap(vars);

        //create a JSON object for each possible solution sequence
        for (Iterator<Binding> i = solutionSequences.rows(); i.hasNext();){
            Map<String, com.aql.querytree.expressions.Expr> objectProperties = new HashMap<>();
            Binding b = i.next();
            for(Var var : vars){
                Node value = b.get(var);

                //if a value for a variable is UNDEF, then in the boundVars map set that variable to canBeNull = true
                if(value == null) {
                    boundVars.get(var.getVarName()).updateCanBeNull(true);
                    continue;
                }

                objectProperties.put(var.getVarName(), RewritingUtils.ValuesRdfNodeToArangoObject(value));
            }

            listOfAqlObjects.add(new Const_Object(objectProperties));
        }

        Const_Array array = new Const_Array(listOfAqlObjects.toArray(new Constant[listOfAqlObjects.size()]));
        AqlQueryNode forLoop;
        if(listOfAqlObjects.size() == 0){
            forLoop = new IterationResource(forLoopVarGenerator.getNew(), array);
            boundVars = new HashMap<>();
        }
        else {
            forLoop = AddNewAssignmentAndLoop(array, boundVars);
        }

        createdAqlNodes.add(forLoop);
        boundSparqlVariablesByOp.setSparqlVariablesByOp(opTable, boundVars);
    }

    protected void AddNamedGraphFilters(String forLoopVarName, ExprList filterConditions){
        if(namedGraphNames.isEmpty()){
            //TODO what if there are no named graphs defined in FROM NAMED??? in that case the inner graph pattern should return nothing no??
            // or we should consider only triples that are within any named graph.. ie. graph attribute not null or empty
        }

        AddGraphFilters(namedGraphNames, forLoopVarName, filterConditions);
    }

    protected void AddDefaultGraphFilters(String forLoopVarName, ExprList filterConditions){
        AddGraphFilters(defaultGraphNames, forLoopVarName, filterConditions);
    }
    /**
     * This method takes a list of graphs specified in FROM clauses OR a list of graphs specified in FROM NAMED clauses
     * and adds filters on the current for loop to make sure only triples in these graphs are considered
     * @param graphNames list of graph uris
     * @param forLoopVarName AQL variable name of current forloop
     * @param filterConditions current list of filter conditions in the for loop
     */
    protected void AddGraphFilters(List<String> graphNames, String forLoopVarName, ExprList filterConditions){
        com.aql.querytree.expressions.Expr filterExpr = null;

        //add filters for default or named graphs
        //TODO consider using POSITION function instead to shorten the filter expr
        for(String g: graphNames){
            com.aql.querytree.expressions.Expr currExpr = new Expr_Equals(new ExprVar(AqlUtils.buildVar(forLoopVarName, ArangoAttributes.GRAPH_NAME, ArangoAttributes.VALUE)), new Const_String(g));

            if(filterExpr == null){
                filterExpr = currExpr;
            }
            else{
                filterExpr = new Expr_LogicalOr(filterExpr, currExpr);
            }
        }

        if(filterExpr != null)
            filterConditions.add(filterExpr);
    }

    protected void AddNewAssignment(Op opToAssign){
        _aqlQueryExpressionTree.add(new AssignedResource(assignmentVarGenerator.getNew(), opToAssign));
    }

    protected void AddNewAssignment(com.aql.querytree.expressions.Expr exprToAssign){
        _aqlQueryExpressionTree.add(new AssignedResource(assignmentVarGenerator.getNew(), exprToAssign));
    }

    protected AqlQueryNode AddNewAssignmentAndLoop(Op opToAssign, Map<String, BoundAqlVars> boundVars){
        //create for loop over query that already had projection by using let stmt
        //Add let stmt to our main query structure
        _aqlQueryExpressionTree.add(new AssignedResource(assignmentVarGenerator.getNew(), opToAssign));
        AqlQueryNode forLoop = new IterationResource(forLoopVarGenerator.getNew(), new ExprVar(assignmentVarGenerator.getCurrent()));
        //update bound vars
        RewritingUtils.UpdateBoundVariablesMapping(boundVars, forLoopVarGenerator.getCurrent(), true);
        return forLoop;
    }

    protected AqlQueryNode AddNewAssignmentAndLoop(com.aql.querytree.expressions.Expr exprToAssign, Map<String, BoundAqlVars> boundVars){
        //create for loop over query that already had projection by using let stmt
        //Add let stmt to our main query structure
        _aqlQueryExpressionTree.add(new AssignedResource(assignmentVarGenerator.getNew(), exprToAssign));
        AqlQueryNode forLoop = new IterationResource(forLoopVarGenerator.getNew(), new ExprVar(assignmentVarGenerator.getCurrent()));
        //update bound vars
        RewritingUtils.UpdateBoundVariablesMapping(boundVars, forLoopVarGenerator.getCurrent(), true);
        return forLoop;
    }

    protected AqlQueryNode EnsureIteration(AqlQueryNode node, Map<String, BoundAqlVars> boundVars){
        if(node instanceof com.aql.querytree.operators.OpProject){
            node = AddNewAssignmentAndLoop((Op)node, boundVars);
        }

        return node;
    }

    protected AqlQueryNode EnsureIterationResource(AqlQueryNode node, Map<String, BoundAqlVars> boundVars){
        if(!(node instanceof IterationResource)) {
            if(!(node instanceof com.aql.querytree.operators.OpProject)){
                //add project over left op + let stmt and then create for loop which we need
                node = new com.aql.querytree.operators.OpProject(node, RewritingUtils.CreateProjectionVarExprList(boundVars), false);
            }
            node = AddNewAssignmentAndLoop((Op)node, boundVars);
        }

        return node;
    }

    protected AqlQueryNode EnsureProjection(AqlQueryNode node, Map<String, BoundAqlVars> boundVars){
        if(!(node instanceof com.aql.querytree.operators.OpProject)){
            node = new com.aql.querytree.operators.OpProject(node, RewritingUtils.CreateProjectionVarExprList(boundVars), false);
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

    protected com.aql.querytree.expressions.ExprList GetFiltersOnCommonVars(Map<String, BoundAqlVars> leftBoundVars, Map<String, BoundAqlVars> rightBoundVars){
        return RewritingUtils.GetFiltersOnCommonVars(leftBoundVars, rightBoundVars, dataModel);
    }

}

