package com.sparql_to_aql;

import com.aql.algebra.expressions.ExprAggregator;
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
import com.sparql_to_aql.utils.RewritingUtils;
import com.sparql_to_aql.utils.VariableGenerator;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.impl.RDFLangString;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.SortCondition;
import org.apache.jena.sparql.algebra.Table;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpMinus;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.Expr;
import java.util.*;
import java.util.stream.Collectors;

//class used for rewriting of SPARQL algebra query expression to AQL algebra query expression
//translating the SPARQL algebra expressions directly to an AQL query would be hard to re-optimise
//TODO could possibly use WalkerVisitor (to visit both Ops and Exprs in the same class)
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
    private VariableGenerator assignementVarGenerator = new VariableGenerator("assign", "item");

    //TODO use below to keep map of bound variables per op, where the second map is used to map the sparql variable name
    // into the corresponding aql variable name to use (due to for loop variable names)
    //TODO possibly use hashCode of ARQ ops instead of that of AQL... so it's easier to get them when processing the next ARQ op
    //Keep track of which variables have already been bound (or not if optional), by mapping ARQ algebra op hashcode to the list of vars
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
            //do not re-add variables that are already in the list
            variables.keySet().removeAll(currUsedVars.keySet());
            currUsedVars.putAll(variables);
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

    //TODO consider the possibility of replacing BGP with more than 1 triple pattern into multiple joins of triple patterns (remove bgps)
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

            ProcessTripleNode(triple.getSubject(), NodeRole.SUBJECT, iterationVar, filterConditions, usedVars);
            ProcessTripleNode(triple.getPredicate(), NodeRole.PREDICATE, iterationVar, filterConditions, usedVars);
            ProcessTripleNode(triple.getObject(), NodeRole.OBJECT, iterationVar, filterConditions, usedVars);

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
        //TODO BETTER TO NOT USE LET STMTS AND JUST NEST BOTH QUERIES FOR PERFORMANCE... question is how to do it in code

        //if one side of the join is a table, cater for that
        if(opJoin.getLeft() instanceof OpTable){
            OpTable opTable = (OpTable) opJoin.getLeft();
            Map<String, String> boundSparqlVariablesInOpToJoin = GetSparqlVariablesByOp(opJoin.getRight().hashCode());
            opToJoin1 = new com.aql.algebra.operators.OpFilter(ProcessBindingsTableJoin(opTable.getTable(), boundSparqlVariablesInOpToJoin), opToJoin1);
            SetSparqlVariablesByOp(opJoin.hashCode(), boundSparqlVariablesInOpToJoin);
            createdAqlOps.add(opToJoin1);
        }
        else if(opJoin.getRight() instanceof OpTable){
            OpTable opTable = (OpTable) opJoin.getRight();
            Map<String, String> boundSparqlVariablesInOpToJoin = GetSparqlVariablesByOp(opJoin.getLeft().hashCode());
            opToJoin1 = new com.aql.algebra.operators.OpFilter(ProcessBindingsTableJoin(opTable.getTable(), boundSparqlVariablesInOpToJoin), opToJoin1);
            SetSparqlVariablesByOp(opJoin.hashCode(), boundSparqlVariablesInOpToJoin);
            createdAqlOps.add(opToJoin1);
        }
        else{
            //use list of common variables between the resulting "bgps" that must be joined
            //also add used vars in join to sparqlVariablesByOp
            Map<String, String> leftsideVars = GetSparqlVariablesByOp(opJoin.getLeft().hashCode());
            AddSparqlVariablesByOp(opJoin.hashCode(), leftsideVars);
            Map<String, String> rightsideVars = GetSparqlVariablesByOp(opJoin.getRight().hashCode());
            AddSparqlVariablesByOp(opJoin.hashCode(), rightsideVars);

            Set<String> commonVars = leftsideVars.keySet();
            commonVars.retainAll(rightsideVars.keySet());

            Op opToJoin2 = createdAqlOps.removeFirst();

            ExprList filtersExprs = new ExprList();
            for (String commonVar: commonVars){
                filtersExprs.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(leftsideVars.get(commonVar))), com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(leftsideVars.get(commonVar)))));
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
        //iterate over expressions, add filter conditions in AQL format to list for concatenating later
        ExprList filterConds = new ExprList();
        for(Iterator<Expr> i = opFilter.getExprs().iterator(); i.hasNext();){
            filterConds.add(ProcessExpr(i.next()));
        }

        createdAqlOps.add(new com.aql.algebra.operators.OpFilter(filterConds, currOp));
    }

    @Override
    public void visit(OpExtend opExtend){
        Op currOp = createdAqlOps.removeLast();

        VarExprList varExprList = opExtend.getVarExprList();

        List<com.aql.algebra.operators.OpAssign> assignmentExprs = new ArrayList<>();
        varExprList.forEachVarExpr((v,e) -> assignmentExprs.add(new com.aql.algebra.operators.OpAssign(v.getVarName(), ProcessExpr(e))));
        //nest assignments into current op or extend current op

        currOp = new com.aql.algebra.operators.OpExtend(currOp, assignmentExprs);
        createdAqlOps.add(currOp);

        //add variables to sparqlVariablesByOp
        Map<String, String> newBoundVars = new HashMap<>();
        varExprList.forEachVar(v -> newBoundVars.put(v.getName(), v.getName()));
        AddSparqlVariablesByOp(opExtend.getSubOp().hashCode(), newBoundVars);
    }

    @Override
    public void visit(OpUnion opUnion){
        //TODO how we perform this operation depends if the union is between subqueries that have a projection or not..I think we have to add a projection on both..
        //TODO get the subquery for the left and right of the union
        String varname = assignementVarGenerator.getNew();
        //System.out.print("LET unionResult = UNION(left_result_here, right_result_here)");
        com.aql.algebra.operators.OpAssign letStmt = new com.aql.algebra.operators.OpAssign(varname, new Expr_Union(com.aql.algebra.expressions.Var.alloc("left_result_here"), com.aql.algebra.expressions.Var.alloc("right_result_here")));
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
                com.aql.algebra.expressions.VarExprList varExprList = new com.aql.algebra.expressions.VarExprList();
                Map<String, String> aqlVars = boundSparqlVariablesByOp.get(opProject.getSubOp().hashCode());

                for(Var v: projectableVars){
                    //Add each var to collect clause
                    com.aql.algebra.expressions.Var aqlProjectVar = com.aql.algebra.expressions.Var.alloc(v.getVarName());
                    com.aql.algebra.expressions.Expr varExpr = new Expr_Equals(aqlProjectVar, com.aql.algebra.expressions.Var.alloc(aqlVars.get(v.getVarName())));
                    varExprList.add(aqlProjectVar, varExpr);
                }

                //apply collect stmt over current projectionSubOp
                currOp = new OpCollect(currOp, varExprList, null);
            }
        }

        List<com.aql.algebra.expressions.Expr> returnVariables = projectableVars.stream().map(v -> com.aql.algebra.expressions.Var.alloc(v.getVarName()))
                .collect(Collectors.toList());

        Op returnStmt = new com.aql.algebra.operators.OpProject(currOp, returnVariables, useDistinct);
        createdAqlOps.add(returnStmt);
    }

    @Override
    public void visit(OpOrder opOrder) {
        Op orderSubOp = createdAqlOps.removeLast();

        List<SortCondition> sortConditionList = opOrder.getConditions();
        List<com.aql.algebra.SortCondition> aqlSortConds = new ArrayList<>();

        for (int i= 0; i < sortConditionList.size(); i++) {
            SortCondition currCond = sortConditionList.get(i);
            //direction = 1 if ASC, -1 if DESC, -2 if unspecified (default asc)
            com.aql.algebra.SortCondition.Direction direction = currCond.getDirection() == -1 ? com.aql.algebra.SortCondition.Direction.DESC : com.aql.algebra.SortCondition.Direction.ASC;
            //TODO here we're assuming expr is definitely a variable.. would be better to use expression visitor and get resulting AQL expression from it
            aqlSortConds.add(new com.aql.algebra.SortCondition(com.aql.algebra.expressions.Var.alloc(currCond.getExpression().getVarName()), direction));
        }

        OpSort aqlSort = new OpSort(orderSubOp, aqlSortConds);
        //TODO add variables by op
        createdAqlOps.add(aqlSort);
    }

    @Override
    public void visit(OpGroup opGroup){
        Op subOp = createdAqlOps.removeLast();

        VarExprList varExprList = opGroup.getGroupVars();
        //TODO consider just not supporting aggregations for this implementation

        com.aql.algebra.expressions.VarExprList aqlVarExpr = new com.aql.algebra.expressions.VarExprList();
        //varExprList.forEachVarExpr((v,e) -> aqlVarExpr.add(v.getVarName(), RewritingUtils.ProcessExpr(e)));
        OpCollect collectOp = new OpCollect(subOp, aqlVarExpr, null);
        createdAqlOps.add(collectOp);
    }

    @Override
    public void visit(OpSlice opSlice){
        Op currOp = createdAqlOps.removeLast();

        createdAqlOps.add(new OpLimit(currOp, opSlice.getStart(), opSlice.getLength()));
    }

    public static void ProcessTripleNode(Node node, NodeRole role, String forLoopVarName, ExprList filterConditions, Map<String, String> usedVars){
        String attributeName;

        Set<String> usedSparqlVars = usedVars.keySet();

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
        //IMP: in ARQ query expression, blank nodes are represented as variables ??0, ??1 etc.. and an Invalid SPARQL query error is given if same blank node is used in more than one subquery
        String currAqlVarName = AqlUtils.buildVar(forLoopVarName, attributeName);

        if(node.isVariable()) {
            String var_name = node.getName();
            if(usedSparqlVars.contains(var_name)){
                //node was already bound in another triple, add a filter condition instead
                filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(currAqlVarName), com.aql.algebra.expressions.Var.alloc(usedVars.get(var_name))));
            }
            else {
                //add variable to list of already used/bound vars
                usedVars.put(var_name, currAqlVarName);
            }
        }
        else if(node.isURI()){
            filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(currAqlVarName, ArangoAttributes.TYPE)), new Const_String(RdfObjectTypes.IRI)));
            filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(currAqlVarName, ArangoAttributes.VALUE)), new Const_String(node.getURI())));
        }
        else if(node.isLiteral()){
            ProcessLiteralNode(node, currAqlVarName, filterConditions);
        }
    }

    public static void ProcessLiteralNode(Node literal, String currAqlVarName, ExprList filterConditions){
        //important to compare to data type in Arango object here
        filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(currAqlVarName, ArangoAttributes.TYPE)), new Const_String(RdfObjectTypes.LITERAL)));

        RDFDatatype datatype = literal.getLiteralDatatype();
        filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(currAqlVarName, ArangoAttributes.LITERAL_DATA_TYPE)), new Const_String(datatype.getURI())));

        if (datatype instanceof RDFLangString) {
            filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(currAqlVarName, ArangoAttributes.LITERAL_LANGUAGE)), new Const_String(literal.getLiteralLanguage())));
        }

        //deiced which of these 2 below methods to call to get the value - refer to https://www.w3.org/TR/sparql11-query/#matchingRDFLiterals
        //would probably be easier to use the lexical form everywhere.. that way I don't have to parse by type.. although when showing results to user we'll have to customize their displays according to the type..
        //literal.getLiteralValue();
        //TODO using the lexical form won't work when we want to apply math or string functions to values in AQL!
        filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(currAqlVarName, ArangoAttributes.VALUE)), new Const_String(literal.getLiteralLexicalForm())));
    }

    public static ExprList ProcessBindingsTableJoin(Table table, Map<String, String> boundVars){
        //the FILTER commands should be added if there is a JOIN clause between a Bindings table and some other op..
        //thus the functionality below could be changed to represent the Table in Arango (ex. array of objects with the bound vars..)
        com.aql.algebra.expressions.Expr jointFilterExpr = null;
        List<Var> vars = table.getVars();
        for (Iterator<Binding> i = table.rows(); i.hasNext();){
            com.aql.algebra.expressions.Expr currExpr = ProcessBinding(i.next(), vars, boundVars);

            if(jointFilterExpr == null){
                jointFilterExpr = currExpr;
            }
            else{
                jointFilterExpr = new Expr_LogicalOr(jointFilterExpr, currExpr);
            }
        }
        return new ExprList(jointFilterExpr);
    }

    public static com.aql.algebra.expressions.Expr ProcessBinding(Binding binding, List<Var> vars, Map<String, String> boundVars){
        int undefinedVarsAmount = 0;
        com.aql.algebra.expressions.Expr wholeExpr = null;
        for(Var var : vars){
            Node value = binding.get(var);
            com.aql.algebra.expressions.Expr currExpr;
            if(value == null) {
                //the variable is bound to an undefined value, that is the value of that variable can be anything in this binding case
                //if all values in one binding (one row of the table) are all null (UNDEF), then result set shouldn't be filtered
                undefinedVarsAmount++;
                if(undefinedVarsAmount == vars.size()) {
                    currExpr = new Const_Bool(true);
                }
                else{
                    continue;
                }
            }
            else {
                //TODO consider whether the binding is to a literal, or uri.. if literal what data type it has, etc...
                currExpr = new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(boundVars.get(var.getName()))), new Const_String(value.toString()));
            }

            if(wholeExpr == null){
                wholeExpr = currExpr;
            }
            else{
                wholeExpr = new Expr_LogicalAnd(wholeExpr, currExpr);
            }
        }

        return wholeExpr;
    }

    public static com.aql.algebra.expressions.Expr ProcessExpr(Expr expr){
        Expr aqlExpr;

        //TODO remove below when we're constructing actual expr
        Const_Bool test = new Const_Bool(false);

        //TODO use an ExprVisitor here
        return test;
    }

    public void AddGraphFilters(List<String> graphNames, String forLoopVarName, ExprList filterConditions){
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

}

