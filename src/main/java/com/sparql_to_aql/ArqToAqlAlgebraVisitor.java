package com.sparql_to_aql;

import com.aql.algebra.AqlQueryNode;
import com.aql.algebra.expressions.ExprVar;
import com.aql.algebra.expressions.constants.Const_Object;
import com.aql.algebra.operators.*;
import com.aql.algebra.operators.OpSequence;
import com.aql.algebra.resources.AssignedResource;
import com.aql.algebra.resources.IterationResource;
import com.sparql_to_aql.constants.ArangoAttributes;
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
import org.apache.jena.query.Query;
import org.apache.jena.query.SortCondition;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;
import java.util.*;

//class used for rewriting of SPARQL algebra query expression to AQL algebra query expression
public abstract class ArqToAqlAlgebraVisitor extends RewritingOpVisitorBase {

    //private String defaultGraphCollectionOrVarName;

    //Aql query can be made of a sequence of "subqueries" and assignments, hence OpSequence
    protected OpSequence _aqlAlgebraQueryExpressionTree;

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
    //the second map is used to map the sparql variable name  into the corresponding aql variable name to use (due to for loop variable names)
    protected Map<Integer, Map<String, String>> boundSparqlVariablesByOp = new HashMap<>();

    //use linked list - easier to pop out and push items from front or back
    protected LinkedList<AqlQueryNode> createdAqlNodes = new LinkedList<>();

    public ArqToAqlAlgebraVisitor(List<String> defaultGraphNames, List<String> namedGraphs){
        this.defaultGraphNames = defaultGraphNames;
        this.namedGraphNames = namedGraphs;
        this._aqlAlgebraQueryExpressionTree = new OpSequence();
    }

    @Override
    public void visit(OpSlice opSlice){
        AqlQueryNode currOp = createdAqlNodes.removeLast();
        long start = opSlice.getStart();
        if(opSlice.getStart() < 0)
            start = 0;

        boolean projectAfterSlice = false;
        Map<String, String> boundVars = GetSparqlVariablesByOp(opSlice.getSubOp());

        //check if currOp is project, if so add a new for loop with the slicing + project
        if(currOp instanceof com.aql.algebra.operators.OpProject) {
            currOp = AddNewAssignmentAndLoop((Op) currOp, boundVars);
            projectAfterSlice = true;
        }

        Op limitOp = new OpLimit(currOp, start, opSlice.getLength());

        if(projectAfterSlice)
            limitOp = new com.aql.algebra.operators.OpProject(limitOp, new ExprVar(forLoopVarGenerator.getCurrent()), false);

        createdAqlNodes.add(limitOp);
        SetSparqlVariablesByOp(opSlice.hashCode(), boundVars);
    }

    @Override
    public void visit(OpOrder opOrder) {
        AqlQueryNode orderSubOp = createdAqlNodes.removeLast();

        List<SortCondition> sortConditionList = opOrder.getConditions();
        List<com.aql.algebra.SortCondition> aqlSortConds = new ArrayList<>();

        Map<String, String> boundVars = boundSparqlVariablesByOp.get(opOrder.getSubOp().hashCode());

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
        SetSparqlVariablesByOp(opOrder.hashCode(), boundVars);
    }

    @Override
    public void visit(OpProject opProject){
        boolean useDistinct = false;

        AqlQueryNode currOp = createdAqlNodes.removeLast();
        List<Var> projectableVars = opProject.getVars();
        Map<String, String> boundVars = boundSparqlVariablesByOp.get(opProject.getSubOp().hashCode());

        if(currOp instanceof com.aql.algebra.operators.OpProject){
            //TODO ORRR possibly modify the variable expressions in the current project op instead of adding another one??
            currOp = AddNewAssignmentAndLoop((Op)currOp, boundVars);
        }

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
        //TODO consider moving this to a seperate method/class
        Map<String, String> projectedAqlVars = new HashMap<>();
        projectableVars.stream().map(v -> projectedAqlVars.put(v.getVarName(), v.getVarName()));
        boundSparqlVariablesByOp.put(opProject.hashCode(), projectedAqlVars);

        createdAqlNodes.add(returnStmt);
    }

    @Override
    public void visit(OpJoin opJoin){
        AqlQueryNode opToJoin1 = createdAqlNodes.removeLast();
        boolean joinToValuesTable = false;
        OpTable opTable = null;
        Map<String, String> boundVariablesInOp1ToJoin;

        //if one side of the join is a VALUES table, cater for that
        if(opJoin.getLeft() instanceof OpTable){
            joinToValuesTable = true;
            opTable = (OpTable) opJoin.getLeft();
            boundVariablesInOp1ToJoin = GetSparqlVariablesByOp(opJoin.getRight());
        }
        else{
            boundVariablesInOp1ToJoin = GetSparqlVariablesByOp(opJoin.getLeft());
            if(opJoin.getRight() instanceof OpTable){
                joinToValuesTable = true;
                opTable = (OpTable) opJoin.getRight();
            }
        }

        //since joining involves adding filter conditions to match the results of both operators
        //we need two for loops that we can nest.
        //We can't nest a projection op so assign it's projected data to a variable and add a new forloop over that data
        if(opToJoin1 instanceof com.aql.algebra.operators.OpProject){
            opToJoin1 = AddNewAssignmentAndLoop((Op)opToJoin1, boundVariablesInOp1ToJoin);
        }

        if(joinToValuesTable){
            //add new filter conditions to the current op according to the VALUES table
            opToJoin1 = new com.aql.algebra.operators.OpFilter(RewritingUtils.ProcessBindingsTableJoin(opTable.getTable(), boundVariablesInOp1ToJoin), opToJoin1);
            SetSparqlVariablesByOp(opJoin.hashCode(), boundVariablesInOp1ToJoin);
            createdAqlNodes.add(opToJoin1);
        }
        else{
            AqlQueryNode opToJoin2 = createdAqlNodes.removeLast();
            Map<String, String> boundVariablesInOp2ToJoin = GetSparqlVariablesByOp(opJoin.getRight());

            //deal with the projection case as we did with opToJoin1 above
            if(opToJoin2 instanceof com.aql.algebra.operators.OpProject){
                opToJoin2 = AddNewAssignmentAndLoop((Op)opToJoin1, boundVariablesInOp2ToJoin);
            }

            //add used vars in both the graph patterns being joined to sparqlVariablesByOp
            //since by joining we will now have all the variables in this scope
            AddSparqlVariablesByOp(opJoin.hashCode(), boundVariablesInOp1ToJoin);
            AddSparqlVariablesByOp(opJoin.hashCode(), boundVariablesInOp2ToJoin);
            //use list of common variables between the graph patterns that must be joined to add the joining filter conditions
            Set<String> commonVars = MapUtils.GetCommonMapKeys(boundVariablesInOp1ToJoin, boundVariablesInOp2ToJoin);

            if(commonVars.size() > 0) {
                //TODO move this logic to common method?
                ExprList filtersExprs = new ExprList();
                for (String commonVar : commonVars) {
                    filtersExprs.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(boundVariablesInOp1ToJoin.get(commonVar))), com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(boundVariablesInOp2ToJoin.get(commonVar)))));
                }
                opToJoin2 = new com.aql.algebra.operators.OpFilter(filtersExprs, opToJoin2);
            }

            //nest one for loop in the other and add filter statements
            opToJoin1 = new OpNest(opToJoin1, opToJoin2);
            createdAqlNodes.add(opToJoin1);
        }
    }

    @Override
    public void visit(OpFilter opFilter){
        //add aql filter operator over current op
        AqlQueryNode currOp = createdAqlNodes.removeLast();
        Map<String, String> boundVars = GetSparqlVariablesByOp(opFilter.getSubOp());
        //iterate over expressions, add AQL filter conditions to list
        ExprList filterConds = new ExprList();
        for(Iterator<Expr> i = opFilter.getExprs().iterator(); i.hasNext();){
            filterConds.add(RewritingUtils.ProcessExpr(i.next(), boundVars));
        }

        createdAqlNodes.add(new com.aql.algebra.operators.OpFilter(filterConds, currOp));
        SetSparqlVariablesByOp(opFilter.hashCode(), boundVars);
    }

    @Override
    public void visit(OpLeftJoin opLeftJoin){
        AqlQueryNode rightOp = createdAqlNodes.removeLast();
        AqlQueryNode leftOp = createdAqlNodes.removeLast();

        Map<String, String> leftBoundVars = GetSparqlVariablesByOp(opLeftJoin.getLeft());
        Map<String, String> rightBoundVars = GetSparqlVariablesByOp(opLeftJoin.getRight());

        leftOp = EnsureIterationResource(leftOp, leftBoundVars);
        String outerLoopVarName = ((IterationResource) leftOp).getIterationVar().getVarName();

        //add filters on the right side results to make sure common variables match to those on the left
        ExprList filtersExprs = RewritingUtils.GetFiltersOnCommonVars(leftBoundVars, rightBoundVars);

        //TODO check why we are applying them to the optional part below
        //if left join contains exprs, apply filter exprs on optional part
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

        //TODO add comments or an example for why we are doing the below
        AssignedResource innerAssignment = new AssignedResource(assignmentVarGenerator.getNew(), (Op)rightOp);

        AqlQueryNode newOp = new com.aql.algebra.operators.OpNest(leftOp, innerAssignment);

        AqlQueryNode subquery = new IterationResource(forLoopVarGenerator.getNew(), new Expr_Conditional(new Expr_GreaterThan(new Expr_Length(com.aql.algebra.expressions.Var.alloc(assignmentVarGenerator.getCurrent())), new Const_Number(0)), com.aql.algebra.expressions.Var.alloc(assignmentVarGenerator.getCurrent()), new Const_Array(new Const_Object())));
        newOp = new OpNest(newOp, subquery);

        newOp = new com.aql.algebra.operators.OpProject(newOp, new Expr_Merge(com.aql.algebra.expressions.Var.alloc(outerLoopVarName), com.aql.algebra.expressions.Var.alloc(forLoopVarGenerator.getCurrent())),false);

        Map<String, String> boundVars = MapUtils.MergeMapsKeepFirstDuplicateKeyValue(leftBoundVars, rightBoundVars);
        newOp = AddNewAssignmentAndLoop((Op)newOp, boundVars);
        createdAqlNodes.add(newOp);
        //add bound vars to map
        SetSparqlVariablesByOp(opLeftJoin.hashCode(), boundVars);
    }

    //TODO add more comments to union code below
    @Override
    public void visit(OpUnion opUnion){
        //how we perform this operation depends if the union is between subqueries that have a projection or not
        //example of what we need: LET unionResult = UNION(left_result_here, right_result_here);
        AqlQueryNode rightOp = createdAqlNodes.removeLast();
        AqlQueryNode leftOp = createdAqlNodes.removeLast();

        Map<String, String> leftBoundVars = GetSparqlVariablesByOp(opUnion.getLeft());
        Map<String, String> rightBoundVars = GetSparqlVariablesByOp(opUnion.getRight());

        if(!(leftOp instanceof com.aql.algebra.operators.OpProject)){
            leftOp = new com.aql.algebra.operators.OpProject(leftOp, RewritingUtils.CreateProjectionVarExprList(leftBoundVars), false);
        }

        if(!(rightOp instanceof com.aql.algebra.operators.OpProject)){
            rightOp = new com.aql.algebra.operators.OpProject(rightOp, RewritingUtils.CreateProjectionVarExprList(rightBoundVars), false);
        }

        AddNewAssignment((Op)leftOp);
        String leftAssignVar = assignmentVarGenerator.getCurrent();
        AddNewAssignment((Op)rightOp);
        String rightAssignVar = assignmentVarGenerator.getCurrent();

        Map<String, String> allBoundVars = MapUtils.MergeMapsKeepFirstDuplicateKeyValue(leftBoundVars, rightBoundVars);
        createdAqlNodes.add(AddNewAssignmentAndLoop(new Expr_Union(com.aql.algebra.expressions.Var.alloc(leftAssignVar), com.aql.algebra.expressions.Var.alloc(rightAssignVar)), allBoundVars));
        AddSparqlVariablesByOp(opUnion.hashCode(), allBoundVars);
    }

    //TODO add comments to minus code below
    @Override
    public void visit(OpMinus opMinus){
        // add nested forloops for op with filters so we only keep solutions from leftOp that aren't compatible
        // with rightOp, use project distinct to return leftop results from for loop? not sure if this will work
        // actually this might be wrong and we only need one for loop with a filter and then use a for loop within the filter to find all solution mappings in rightOp that are compatible solution mappings from leftOp
        // and we only keep mappings from leftOp were the count() of  items returned by that inner for loop is 0!!!
        // Let s = FOR doc IN triples
        //         FILTER (doc.v1 = leftOp.v1) AND (doc.v2 = leftOp.v2)
        //         COLLECT WITH COUNT INTO length
        //         RETURN length
        // And then FOR x in leftOp
        // FILTER s == 0
        AqlQueryNode rightOp = createdAqlNodes.removeLast();
        AqlQueryNode leftOp = createdAqlNodes.removeLast();

        Map<String, String> leftBoundVars = GetSparqlVariablesByOp(opMinus.getLeft());
        Map<String, String> rightBoundVars = GetSparqlVariablesByOp(opMinus.getRight());

        Set<String> commonVars = MapUtils.GetCommonMapKeys(leftBoundVars, rightBoundVars);

        if(commonVars.size() == 0){
            createdAqlNodes.add(leftOp);
            AddSparqlVariablesByOp(opMinus.hashCode(), leftBoundVars);
            return;
        }

        leftOp = EnsureIterationResource(leftOp, leftBoundVars);
        rightOp = EnsureIterationResource(rightOp, rightBoundVars);

        //TODO move this code to common method
        ExprList filtersExprs = new ExprList();
        for (String commonVar : commonVars) {
            filtersExprs.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(leftBoundVars.get(commonVar))), com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(rightBoundVars.get(commonVar)))));
        }

        rightOp = new com.aql.algebra.operators.OpFilter(filtersExprs, rightOp);
        ExprVar countVar = new ExprVar("length");
        rightOp = new OpCollect(rightOp, countVar);
        rightOp = new com.aql.algebra.operators.OpProject(rightOp, countVar, false);

        //TODO if we start allowing subqueries (AqlQueryNode) in expressions, we wouldn't need an extra assignment here
        leftOp = new OpNest(leftOp, new AssignedResource(assignmentVarGenerator.getNew(), (Op)rightOp));
        leftOp = new com.aql.algebra.operators.OpFilter(new Expr_Equals(new ExprVar(assignmentVarGenerator.getCurrent()), new Const_Number(0)), leftOp);
        createdAqlNodes.add(leftOp);
        AddSparqlVariablesByOp(opMinus.hashCode(), leftBoundVars);
    }

        /*@Override
    public void visit(OpExtend opExtend){
        AqlQueryNode currOp = createdAqlNodes.removeLast();

        VarExprList varExprList = opExtend.getVarExprList();

        Map<String, String> prevBoundVars = GetSparqlVariablesByOp(opExtend.getSubOp().hashCode());

        for(Var v: varExprList.getVars()){
            currOp = new com.aql.algebra.operators.OpNest(currOp, new AssignedResource(v.getVarName(), RewritingUtils.ProcessExpr(varExprList.getExpr(v), prevBoundVars)));
        }

        createdAqlNodes.add(currOp);

        //add variables to sparqlVariablesByOp
        Map<String, String> newBoundVars = new HashMap<>();
        varExprList.forEachVar(v -> newBoundVars.put(v.getName(), v.getName()));
        AddSparqlVariablesByOp(opExtend.hashCode(), newBoundVars);
        AddSparqlVariablesByOp(opExtend.hashCode(), prevBoundVars);
    }*/

    /**
     * Add map of bound SPARQL to AQL variables in the scope of a particular SPARQL operator
     * @param opHashCode hashcode of SPARQL operator
     * @param variables map of bound SPARQL to AQL variables
     */
    protected void AddSparqlVariablesByOp(Integer opHashCode, Map<String, String> variables){
        Map<String, String> currUsedVars = GetSparqlVariablesByOp(opHashCode);
        if(currUsedVars == null || currUsedVars.size() == 0) {
            boundSparqlVariablesByOp.put(opHashCode, variables);
        }
        else {
            MapUtils.MergeMapsKeepFirstDuplicateKeyValue(currUsedVars, variables);
        }
    }

    protected void SetSparqlVariablesByOp(Integer opHashCode, Map<String, String> variables){
        boundSparqlVariablesByOp.put(opHashCode, variables);
    }

    protected Map<String, String> GetSparqlVariablesByOp(org.apache.jena.sparql.algebra.Op sparqlOp){
        return GetSparqlVariablesByOp(sparqlOp.hashCode());
    }

    protected Map<String, String> GetSparqlVariablesByOp(Integer opHashCode){
        Map<String, String> currUsedVars = boundSparqlVariablesByOp.get(opHashCode);
        if(currUsedVars == null)
            return new HashMap<>();

        return currUsedVars;
    }

    //TODO add comments for below methods
    protected void AddGraphFilters(List<String> graphNames, String forLoopVarName, ExprList filterConditions){
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

    protected void AddNewAssignment(Op opToAssign){
        _aqlAlgebraQueryExpressionTree.add(new AssignedResource(assignmentVarGenerator.getNew(), opToAssign));
    }

    protected void AddNewAssignment(com.aql.algebra.expressions.Expr exprToAssign){
        _aqlAlgebraQueryExpressionTree.add(new AssignedResource(assignmentVarGenerator.getNew(), exprToAssign));
    }

    protected AqlQueryNode AddNewAssignmentAndLoop(Op opToAssign, Map<String, String> boundVars){
        //create for loop over query that already had projection by using let stmt
        //Add let stmt to our main query structure
        _aqlAlgebraQueryExpressionTree.add(new AssignedResource(assignmentVarGenerator.getNew(), opToAssign));
        AqlQueryNode forLoop = new IterationResource(forLoopVarGenerator.getNew(), com.aql.algebra.expressions.Var.alloc(assignmentVarGenerator.getCurrent()));
        //update bound vars
        RewritingUtils.UpdateBoundVariablesMapping(boundVars, forLoopVarGenerator.getCurrent());
        return forLoop;
    }

    protected AqlQueryNode AddNewAssignmentAndLoop(com.aql.algebra.expressions.Expr exprToAssign, Map<String, String> boundVars){
        //create for loop over query that already had projection by using let stmt
        //Add let stmt to our main query structure
        _aqlAlgebraQueryExpressionTree.add(new AssignedResource(assignmentVarGenerator.getNew(), exprToAssign));
        AqlQueryNode forLoop = new IterationResource(forLoopVarGenerator.getNew(), com.aql.algebra.expressions.Var.alloc(assignmentVarGenerator.getCurrent()));
        //update bound vars
        RewritingUtils.UpdateBoundVariablesMapping(boundVars, forLoopVarGenerator.getCurrent());
        return forLoop;
    }

    //TODO check where we're using this method.. do we really need an iteration resource or can a forloop with filters etc. attached still be considered?
    protected AqlQueryNode EnsureIterationResource(AqlQueryNode node, Map<String, String> boundVars){
        if(!(node instanceof IterationResource)) {
            if(!(node instanceof com.aql.algebra.operators.OpProject)){
                //add project over left op + let stmt and then create for loop which we need
                node = new com.aql.algebra.operators.OpProject(node, RewritingUtils.CreateProjectionVarExprList(boundVars), false);
            }
            node = AddNewAssignmentAndLoop((Op)node, boundVars);
        }

        return node;
    }

}

