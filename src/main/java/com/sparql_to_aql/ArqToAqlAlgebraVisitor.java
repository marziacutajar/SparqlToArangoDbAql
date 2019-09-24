package com.sparql_to_aql;

import com.aql.algebra.AqlQueryNode;
import com.aql.algebra.expressions.constants.Const_Object;
import com.aql.algebra.operators.*;
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
//translating the SPARQL algebra expressions directly to an AQL query would be hard to re-optimise
//TODO decide whether to add visit methods for OpList, OpPath and others.. or whether they'll be unsupported
public abstract class ArqToAqlAlgebraVisitor extends RewritingOpVisitorBase {

    //private String defaultGraphCollectionOrVarName;

    //Aql query can be made of a sequence of "subqueries" and assignments, hence the list
    protected List<AqlQueryNode> _aqlAlgebraQueryExpressionTree;

    List<String> defaultGraphNames;
    List<String> namedGraphNames;

    //This method is to be called after the visitor has been used
    public List<AqlQueryNode> GetAqlAlgebraQueryExpression()
    {
        _aqlAlgebraQueryExpressionTree.add(createdAqlNodes.getFirst());

        return _aqlAlgebraQueryExpressionTree;
    }

    protected VariableGenerator forLoopVarGenerator = new VariableGenerator("forloop", "item");
    protected VariableGenerator assignmentVarGenerator = new VariableGenerator("assign", "item");
    protected VariableGenerator graphForLoopVertexVarGenerator = new VariableGenerator("g_v");
    protected VariableGenerator graphForLoopEdgeVarGenerator = new VariableGenerator("g_e");
    protected VariableGenerator graphForLoopPathVarGenerator = new VariableGenerator("g_p");

    //Keep track of which variables have already been bound (or not if optional), by mapping ARQ algebra op hashcode to the list of vars
    //the second map is used to map the sparql variable name  into the corresponding aql variable name to use (due to for loop variable names)
    protected Map<Integer, Map<String, String>> boundSparqlVariablesByOp = new HashMap<>();

    //use linked list - easier to pop out and push items from front or back
    protected LinkedList<AqlQueryNode> createdAqlNodes = new LinkedList<>();

    public ArqToAqlAlgebraVisitor(List<String> defaultGraphNames, List<String> namedGraphs){
        this.defaultGraphNames = defaultGraphNames;
        this.namedGraphNames = namedGraphs;
        this._aqlAlgebraQueryExpressionTree = new ArrayList<>();
    }

    @Override
    public void visit(OpSlice opSlice){
        AqlQueryNode currOp = createdAqlNodes.removeLast();
        long start = opSlice.getStart();
        if(opSlice.getStart() < 0)
            start = 0;

        createdAqlNodes.add(new OpLimit(currOp, start, opSlice.getLength()));
        SetSparqlVariablesByOp(opSlice.hashCode(), GetSparqlVariablesByOp(opSlice.getSubOp().hashCode()));
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

            aqlSortConds.add(new com.aql.algebra.SortCondition(RewritingUtils.ProcessExpr(currCond.getExpression(), boundVars), direction));
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
            currOp = AddNewAssignmentAndLoop((Op)currOp, boundVars);
        }

        if(opProject instanceof OpDistinctProject){
            if(projectableVars.size() == 1){
                useDistinct = true;
            }
            else{
                //SELECT DISTINCT WITH >1 VAR = COLLECT in AQL... consider mentioning this in thesis writeup in AQL algebra

                //apply collect stmt over current projectionSubOp
                currOp = new OpCollect(currOp, RewritingUtils.CreateCollectVarExprList(projectableVars, boundVars), null);
            }
        }

        com.aql.algebra.expressions.VarExprList returnVariables = RewritingUtils.CreateProjectionVarExprList(projectableVars, boundVars);

        Op returnStmt = new com.aql.algebra.operators.OpProject(currOp, returnVariables, useDistinct);

        Map<String, String> projectedAqlVars = new HashMap<>();
        projectableVars.stream().map(v -> projectedAqlVars.put(v.getVarName(), v.getVarName()));
        boundSparqlVariablesByOp.put(opProject.hashCode(), projectedAqlVars);

        createdAqlNodes.add(returnStmt);
    }

    @Override
    public void visit(OpJoin opJoin){
        AqlQueryNode opToJoin1 = createdAqlNodes.removeFirst();
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
        else{
            boundVariablesInOp1ToJoin = GetSparqlVariablesByOp(opJoin.getLeft().hashCode());
        }

        if(opToJoin1 instanceof com.aql.algebra.operators.OpProject){
            opToJoin1 = AddNewAssignmentAndLoop((Op)opToJoin1, boundVariablesInOp1ToJoin);
        }

        if(joinToValuesTable){
            opToJoin1 = new com.aql.algebra.operators.OpFilter(RewritingUtils.ProcessBindingsTableJoin(opTable.getTable(), boundVariablesInOp1ToJoin), opToJoin1);
            SetSparqlVariablesByOp(opJoin.hashCode(), boundVariablesInOp1ToJoin);
            createdAqlNodes.add(opToJoin1);
        }
        else{
            AqlQueryNode opToJoin2 = createdAqlNodes.removeFirst();
            Map<String, String> boundVariablesInOp2ToJoin = GetSparqlVariablesByOp(opJoin.getRight().hashCode());

            if(opToJoin2 instanceof com.aql.algebra.operators.OpProject){
                opToJoin2 = AddNewAssignmentAndLoop((Op)opToJoin1, boundVariablesInOp2ToJoin);
            }

            //use list of common variables between the resulting "bgps" that must be joined
            //also add used vars in join to sparqlVariablesByOp
            AddSparqlVariablesByOp(opJoin.hashCode(), boundVariablesInOp1ToJoin);
            AddSparqlVariablesByOp(opJoin.hashCode(), boundVariablesInOp2ToJoin);

            Set<String> commonVars = MapUtils.GetCommonMapKeys(boundVariablesInOp1ToJoin, boundVariablesInOp2ToJoin);

            if(commonVars.size() > 0) {
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
        //add filter operator over current op
        AqlQueryNode currOp = createdAqlNodes.removeLast();
        Map<String, String> boundVars = GetSparqlVariablesByOp(opFilter.getSubOp().hashCode());
        //iterate over expressions, add filter conditions in AQL format to list for concatenating later
        ExprList filterConds = new ExprList();

        for(Iterator<Expr> i = opFilter.getExprs().iterator(); i.hasNext();){
            filterConds.add(RewritingUtils.ProcessExpr(i.next(), boundVars));
        }

        createdAqlNodes.add(new com.aql.algebra.operators.OpFilter(filterConds, currOp));
        SetSparqlVariablesByOp(opFilter.hashCode(), boundVars);
    }

    @Override
    public void visit(OpLeftJoin opLeftJoin){
        AqlQueryNode leftOp = createdAqlNodes.removeFirst();
        AqlQueryNode rightOp = createdAqlNodes.removeFirst();

        Map<String, String> leftBoundVars = GetSparqlVariablesByOp(opLeftJoin.getLeft().hashCode());
        Map<String, String> rightBoundVars = GetSparqlVariablesByOp(opLeftJoin.getRight().hashCode());

        if(!(leftOp instanceof IterationResource)) {
            if(!(leftOp instanceof com.aql.algebra.operators.OpProject)){
                //add project over left op + let stmt and then create for loop which we need
                leftOp = new com.aql.algebra.operators.OpProject(leftOp, RewritingUtils.CreateProjectionVarExprList(leftBoundVars), false);
            }
            leftOp = AddNewAssignmentAndLoop((Op)leftOp, leftBoundVars);
        }

        //add filters on the right side results to make sure common variables match to those on the left
        ExprList filtersExprs = RewritingUtils.GetFiltersOnCommonVars(leftBoundVars, rightBoundVars);

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

        AssignedResource innerAssignment = new AssignedResource(assignmentVarGenerator.getNew(), (Op)rightOp);

        AqlQueryNode newOp = new com.aql.algebra.operators.OpNest(leftOp, innerAssignment);
        String outerLoopVarName = forLoopVarGenerator.getCurrent();

        AqlQueryNode subquery = new IterationResource(forLoopVarGenerator.getNew(), new Expr_Conditional(new Expr_GreaterThan(new Expr_Length(com.aql.algebra.expressions.Var.alloc(assignmentVarGenerator.getCurrent())), new Const_Number(0)), com.aql.algebra.expressions.Var.alloc(assignmentVarGenerator.getCurrent()), new Const_Array(new Const_Object())));
        newOp = new OpNest(newOp, subquery);

        newOp = new com.aql.algebra.operators.OpProject(newOp, new Expr_Merge(com.aql.algebra.expressions.Var.alloc(outerLoopVarName), com.aql.algebra.expressions.Var.alloc(forLoopVarGenerator.getCurrent())),false);

        Map<String, String> boundVars = MapUtils.MergeMapsKeepFirstDuplicateKeyValue(leftBoundVars, rightBoundVars);
        newOp = AddNewAssignmentAndLoop((Op)newOp, boundVars);
        createdAqlNodes.push(newOp);
        //add bound vars to map
        SetSparqlVariablesByOp(opLeftJoin.hashCode(), boundVars);
    }

    @Override
    public void visit(OpUnion opUnion){
        //how we perform this operation depends if the union is between subqueries that have a projection or not
        AqlQueryNode leftOp = createdAqlNodes.removeFirst();
        AqlQueryNode rightOp = createdAqlNodes.removeFirst();

        Map<String, String> leftBoundVars = GetSparqlVariablesByOp(opUnion.getLeft().hashCode());
        Map<String, String> rightBoundVars = GetSparqlVariablesByOp(opUnion.getRight().hashCode());

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
        //TODO consider using APPEND operator instead of UNION in AQL to keep any sorting order applied before
        //System.out.print("LET unionResult = UNION(left_result_here, right_result_here)");
        createdAqlNodes.push(AddNewAssignmentAndLoop(new Expr_Union(com.aql.algebra.expressions.Var.alloc(leftAssignVar), com.aql.algebra.expressions.Var.alloc(rightAssignVar)), allBoundVars));
        AddSparqlVariablesByOp(opUnion.hashCode(), allBoundVars);
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

    protected Map<String, String> GetSparqlVariablesByOp(Integer opHashCode){
        Map<String, String> currUsedVars = boundSparqlVariablesByOp.get(opHashCode);
        if(currUsedVars == null)
            return new HashMap<>();

        return currUsedVars;
    }

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

}

