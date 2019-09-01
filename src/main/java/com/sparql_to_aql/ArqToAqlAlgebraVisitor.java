package com.sparql_to_aql;

import com.aql.algebra.operators.*;
import com.aql.algebra.operators.OpAssign;
import com.sparql_to_aql.constants.ArangoAttributes;
import com.sparql_to_aql.constants.ArangoDatabaseSettings;
import com.sparql_to_aql.constants.NodeRole;
import com.sparql_to_aql.constants.RdfObjectTypes;
import com.sparql_to_aql.constants.arangodb.AqlOperators;
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
public class ArqToAqlAlgebraVisitor extends RewritingOpVisitorBase {

    private String defaultGraphCollectionOrVarName;

    //Aql query can be made of a sequence of "subqueries" and assignments, hence the list
    private List<Op> _aqlAlgebraQueryExpression;

    //This method is to be called after the visitor has been used
    public List<Op> GetAqlAlgebraQueryExpression()
    {
        return _aqlAlgebraQueryExpression;
    }

    private VariableGenerator forLoopVarGenerator = new VariableGenerator("forloop", "item");
    private VariableGenerator assignementVarGenerator = new VariableGenerator("assign", "item");

    //Keep track of which variables have already been bound (or not if optional), by mapping ARQ algebra op hashcode to the list of vars
    private Map<Integer, List<String>> sparqlVariablesByOp = new HashMap<>();

    //TODO could use Stack or LinkedList instead if necessary
    private List<Op> createdAqlOps = new ArrayList<>();

    public ArqToAqlAlgebraVisitor(List<String> defaultGraphNames, List<String> namedGraphs){
        if(defaultGraphNames.size() == 0){
            defaultGraphCollectionOrVarName = ArangoDatabaseSettings.rdfCollectionName;
        }
        else{
            String forLoopItemName = forLoopVarGenerator.getNew();
            Op forloopOp = new OpFor(forLoopItemName, com.aql.algebra.expressions.Var.alloc(ArangoDatabaseSettings.rdfCollectionName));
            //add filter conds to get only triples in the specified graphs
            ExprList filterConditions = new ExprList();
            for(String graphName: defaultGraphNames){
                filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(forLoopItemName, ArangoAttributes.GRAPH_NAME, ArangoAttributes.VALUE)), new Const_String(graphName)));
            }

            forloopOp = new com.aql.algebra.operators.OpFilter(filterConditions, forloopOp);
            forloopOp = new com.aql.algebra.operators.OpProject(forloopOp, com.aql.algebra.expressions.Var.alloc(forLoopItemName), false);

            //add LET stmt to query so we can use this variable containing all triples in the default graph
            String defaultGraphVarName = "defaultGraph";
            _aqlAlgebraQueryExpression.add(new OpAssign(defaultGraphVarName, forloopOp));
            defaultGraphCollectionOrVarName = defaultGraphVarName;
        }

        for(String ng: namedGraphs){
            //TODO add let stmt for each named graph? or one let stmt with all named graphs together?
        }
    }

    private void AddSparqlVariablesByOp(Integer opHashCode, List<String> variables){
        List<String> currUsedVars = GetSparqlVariablesByOp(opHashCode);
        if(currUsedVars == null) {
            sparqlVariablesByOp.put(opHashCode, variables);
        }
        else {
            //do not re-add variables that are already in the list
            variables.removeAll(currUsedVars);
            currUsedVars.addAll(variables);
        }
    }

    private void SetSparqlVariablesByOp(Integer opHashCode, List<String> variables){
        sparqlVariablesByOp.put(opHashCode, variables);
    }

    private List<String> GetSparqlVariablesByOp(Integer opHashCode){
        List<String> currUsedVars = sparqlVariablesByOp.get(opHashCode);
        if(currUsedVars == null)
            return new ArrayList<>();

        return currUsedVars;
    }

    //TODO consider creating new OpNest operator that shows that some subqueries are nested in another
    //TODO consider the possibility of replacing BGP with more than 1 triple pattern into multiple joins of triple patterns (remove bgps)
    @Override
    public void visit(OpBGP opBgp){
        if(opBgp instanceof OpGraphBGP){
            OpGraphBGP graphBGP = (OpGraphBGP) opBgp;
            Node graphNode = graphBGP.getGraphNode();
            if(graphNode.isVariable()){
                //TODO bind it below
            }
            //TODO add extra filters or assignment
        }

        OpNesting currAqlOp = null;
        List<String> usedVars = new ArrayList<>();
        for(Triple triple : opBgp.getPattern().getList()){
            //keep list of FILTER clauses per triple
            ExprList filterConditions = new ExprList();
            //keep list of LET clauses per triple
            List<com.aql.algebra.operators.OpAssign> assignments = new ArrayList<>();

            String iterationVar = forLoopVarGenerator.getNew();
            Op aqlOp = new OpFor(iterationVar, com.aql.algebra.expressions.Var.alloc("collectionOrVarName_here"));

            ProcessTripleNode(triple.getSubject(), NodeRole.SUBJECT, aqlOp, iterationVar, filterConditions, assignments, usedVars);
            ProcessTripleNode(triple.getPredicate(), NodeRole.PREDICATE, aqlOp, iterationVar, filterConditions, assignments, usedVars);
            ProcessTripleNode(triple.getObject(), NodeRole.OBJECT, aqlOp, iterationVar, filterConditions, assignments, usedVars);

            com.aql.algebra.operators.OpFilter filterOp = new com.aql.algebra.operators.OpFilter(filterConditions, aqlOp);
            if(assignments.size() > 0){
                filterOp.addNestedOps(assignments);
            }

            if(currAqlOp == null) {
                currAqlOp = filterOp;
            }
            else {
                currAqlOp.addNestedOp(filterOp);
            }
        }

        //add used vars in bgp to list
        SetSparqlVariablesByOp(opBgp.hashCode(), usedVars);
        createdAqlOps.add(currAqlOp);
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

    //TODO consider using transformer to remove joins completely if it's between bgps..
    @Override
    public void visit(OpJoin opJoin){
        System.out.println("Entering join");
        Op opToJoin1 = createdAqlOps.get(0);
        String letName1 = assignementVarGenerator.getNew();
        //TODO have to add project to op before using assign

        //TODO insert let stmt in aql query before the join loop
        com.aql.algebra.operators.OpAssign let1 = new com.aql.algebra.operators.OpAssign(letName1, opToJoin1);

        String forLoopName1 = forLoopVarGenerator.getNew();
        OpNesting forLoop1 = new OpFor(forLoopName1, com.aql.algebra.expressions.Var.alloc(letName1));

        //if one side of the join is a table, cater for that
        if(opJoin.getLeft() instanceof OpTable){
            //TODO possibly instead of passing forloop var name directly to methods for usage, put a placeholder and then replace
            // all placeholders with the actual variable names after whole query construction (by keeping a map of varname to op)
            OpTable opTable = (OpTable) opJoin.getLeft();

            //List<String> varsToProject = GetSparqlVariablesByOp(opJoin.getRight().hashCode());
            //TODO we need to know the name of the for loop var in the opToJoin
            //new com.aql.algebra.operators.OpProject()
            forLoop1 = new com.aql.algebra.operators.OpFilter(ProcessBindingsTableJoin(opTable.getTable(), forLoopName1), forLoop1);
            SetSparqlVariablesByOp(opJoin.hashCode(), GetSparqlVariablesByOp(opJoin.getRight().hashCode()));
            createdAqlOps.set(0, forLoop1);
        }
        else if(opJoin.getRight() instanceof OpTable){
            OpTable opTable = (OpTable) opJoin.getRight();
            forLoop1 = new com.aql.algebra.operators.OpFilter(ProcessBindingsTableJoin(opTable.getTable(), forLoopName1), forLoop1);
            SetSparqlVariablesByOp(opJoin.hashCode(), GetSparqlVariablesByOp(opJoin.getLeft().hashCode()));
            createdAqlOps.set(0, forLoop1);
        }
        else{
            //use list of common variables between the resulting "bgps" that must be joined
            List<String> leftsideVars = GetSparqlVariablesByOp(opJoin.getLeft().hashCode());
            AddSparqlVariablesByOp(opJoin.hashCode(), leftsideVars);
            List<String> rightsideVars = GetSparqlVariablesByOp(opJoin.getRight().hashCode());
            AddSparqlVariablesByOp(opJoin.hashCode(), rightsideVars);

            List<String> commonVars = leftsideVars;
            commonVars.retainAll(rightsideVars);

            //OR use ATTRIBUTES function in AQL over both of them to join on the common attrs found
            //and create a FILTER statement with them and then merge variables in both

            Op opToJoin2 = createdAqlOps.get(1);
            String letName2 = assignementVarGenerator.getNew();
            //TODO have to add project to op before using assign
            com.aql.algebra.operators.OpAssign let2 = new com.aql.algebra.operators.OpAssign(letName2, opToJoin2);

            String forLoopName2 = forLoopVarGenerator.getNew();
            Op forLoop2 = new OpFor(forLoopName2, com.aql.algebra.expressions.Var.alloc(letName2));

            ExprList filtersExprs = new ExprList();
            for (String commonVar: commonVars){
                filtersExprs.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(forLoopName1, commonVar)), com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(forLoopName2, commonVar))));
            }

            //nest one for loop in the other and add filter statements
            forLoop1.addNestedOp(new com.aql.algebra.operators.OpFilter(filtersExprs, forLoop2));
            createdAqlOps.set(0, forLoop1);
            createdAqlOps.remove(1);

            //TODO add used vars in join to sparqlVariablesByOp
            //TODO
            //System.out.println("RETURN { var1 = left_side.var1, var2 = leftside.var2, var3 = rightside.var3}");
        }
    }

    //TODO this is not directly supported by AQL..requires using different algebra operator??
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

        Op subquery2 = new OpFor("right_result_to_join", new Expr_Conditional(new Expr_GreaterThan(new Expr_Length(com.aql.algebra.expressions.Var.alloc("filtered_right_side")), new Const_Number(0)), com.aql.algebra.expressions.Var.alloc("filtered_right_side"), new Const_Array()));

        //TODO do below
        //subquery2 = new com.aql.algebra.operators.OpProject(subquery2, new VarExpr());
        //System.out.println("FOR right_result_to_join IN (LENGTH(filtered_right_side) > 0 ? filtered_right_side : [{}])");
        //System.out.println("RETURN { left: x, right: right_result_to_join}");
        //TODO consider using MERGE function...
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
        //add filter operator over current op TODO should we apply this over the outer loop or nested within...
        int currOpIndex = createdAqlOps.size()-1;
        Op currOp = createdAqlOps.get(currOpIndex);
        //iterate over expressions, add filter conditions in AQL format to list for concatenating later
        ExprList filterConds = new ExprList();
        for(Iterator<Expr> i = opFilter.getExprs().iterator(); i.hasNext();){
            filterConds.add(ProcessExpr(i.next()));
        }

        createdAqlOps.set(currOpIndex, new com.aql.algebra.operators.OpFilter(filterConds, currOp));
    }

    @Override
    public void visit(OpExtend opExtend){
        int currOpIndex = createdAqlOps.size()-1;
        Op currOp = createdAqlOps.get(currOpIndex);

        VarExprList varExprList = opExtend.getVarExprList();

        List<com.aql.algebra.operators.OpAssign> assignmentExprs = new ArrayList<>();
        varExprList.forEachVarExpr((v,e) -> assignmentExprs.add(new com.aql.algebra.operators.OpAssign(v.getVarName(), ProcessExpr(e))));
        //TODO nest assignments into current op

        //add variables to sparqlVariablesByOp
        List<String> varList = new ArrayList<>();
        varExprList.forEachVar(v -> varList.add(v.getName()));
        AddSparqlVariablesByOp(opExtend.getSubOp().hashCode(), varList);
    }

    @Override
    public void visit(OpUnion opUnion){
        //TODO how we perform this operation depends if the union is between subqueries that have a projection or not
        //TODO get the subquery for the left and right of the union
        String varname = assignementVarGenerator.getNew();
        //System.out.print("LET unionResult = UNION(left_result_here, right_result_here)");
        com.aql.algebra.operators.OpAssign letStmt = new com.aql.algebra.operators.OpAssign(varname, new Expr_Union(com.aql.algebra.expressions.Var.alloc("left_result_here"), com.aql.algebra.expressions.Var.alloc("right_result_here")));
    }

    @Override
    public void visit(OpProject opProject){
        boolean useDistinct = false;

        int currOpIndex = createdAqlOps.size()-1;
        Op projectionSubOp = createdAqlOps.get(currOpIndex);
        List<Var> projectableVars = opProject.getVars();

        if(opProject instanceof OpDistinctProject){
            if(projectableVars.size() == 1){
                useDistinct = true;
            }
            else{
                //SELECT DISTINCT WITH >1 VAR = COLLECT in AQL... consider mentioning this in thesis writeup in AQL algebra
                com.aql.algebra.expressions.VarExprList varExprList = new com.aql.algebra.expressions.VarExprList();
                for(Var v: projectableVars){
                    //Add each var to collect clause
                    //TODO remember that when assigning it we have to use for loop over the query results and use the forloop item name
                    com.aql.algebra.expressions.Var aqlVar = com.aql.algebra.expressions.Var.alloc(v.getVarName());
                    com.aql.algebra.expressions.Expr varExpr = new Expr_Equals(aqlVar, aqlVar);
                    varExprList.add(aqlVar, varExpr);
                }

                //apply collect stmt over current projectionSubOp
                projectionSubOp = new OpCollect(projectionSubOp, varExprList, null);
            }
        }

        List<com.aql.algebra.expressions.Expr> returnVariables = projectableVars.stream().map(v -> com.aql.algebra.expressions.Var.alloc(v.getVarName()))
                .collect(Collectors.toList());

        Op returnStmt = new com.aql.algebra.operators.OpProject(projectionSubOp, returnVariables, useDistinct);
        createdAqlOps.set(currOpIndex, returnStmt);
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

    @Override
    public void visit(OpGroup opGroup){
        //TODO group, and consider mathematical expressions
        List<String> collectClauses = new ArrayList<>();
        VarExprList varExprList = opGroup.getGroupVars();
        varExprList.forEachVarExpr((v,e) -> collectClauses.add(v.getVarName() + " = " + RewritingUtils.ProcessExpr(e)));
    }

    @Override
    public void visit(OpSlice opSlice){
        int currOpIndex = createdAqlOps.size()-1;
        Op currOp = createdAqlOps.get(currOpIndex);

        createdAqlOps.set(currOpIndex, new OpLimit(currOp, opSlice.getStart(), opSlice.getLength()));
    }

    //TODO decide whether to add visit methods for OpList, OpPath and others.. or whether they'll be unsupported

    public static void ProcessTripleNode(Node node, NodeRole role, Op currOp, String forLoopVarName, ExprList filterConditions, List<com.aql.algebra.operators.OpAssign> assignments, List<String> usedVars){
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
                filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(forLoopVarName,attributeName)), com.aql.algebra.expressions.Var.alloc(var_name)));
            }
            else {
                assignments.add(new com.aql.algebra.operators.OpAssign(node.getName(), com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(forLoopVarName, attributeName))));
                //add variable to list of already used/bound vars
                usedVars.add(node.getName());
            }
        }
        else if(node.isURI()){
            filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(forLoopVarName, attributeName, ArangoAttributes.TYPE)), new Const_String(RdfObjectTypes.IRI)));
            filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(forLoopVarName, attributeName, ArangoAttributes.VALUE)), new Const_String(node.getURI())));
        }
        else if(node.isBlank()){
            //blank nodes act as variables, not much difference other than that the same blank node label cannot be used
            //in two different basic graph patterns in the same query. But it's unclear whether blank nodes can still be projecte... maybe consider that they aren't for this impl
            //indicated by either the label form, such as "_:abc", or the abbreviated form "[]"
            //TODO if blank node doesn't have label, use node.getBlankNodeId();
            System.out.println("ID " + node.getBlankNodeId());
            //TODO also check that blank node label wasn't already used in some other graph pattern
            assignments.add(new com.aql.algebra.operators.OpAssign(node.getBlankNodeLabel(), com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(forLoopVarName, attributeName))));
        }
        else if(node.isLiteral()){
            ProcessLiteralNode(node, forLoopVarName, filterConditions);
        }
    }

    public static void ProcessLiteralNode(Node literal, String forLoopVarName, ExprList filterConditions){
        //important to compare to data type in Arango object here
        filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(forLoopVarName, ArangoAttributes.OBJECT, ArangoAttributes.TYPE)), new Const_String(RdfObjectTypes.LITERAL)));

        RDFDatatype datatype = literal.getLiteralDatatype();
        filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(forLoopVarName, ArangoAttributes.OBJECT, ArangoAttributes.LITERAL_DATA_TYPE)), new Const_String(datatype.getURI())));

        if (datatype instanceof RDFLangString) {
            filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(forLoopVarName, ArangoAttributes.OBJECT, ArangoAttributes.LITERAL_LANGUAGE)), new Const_String(literal.getLiteralLanguage())));
        }

        //deiced which of these 2 below methods to call to get the value - refer to https://www.w3.org/TR/sparql11-query/#matchingRDFLiterals
        //would probably be easier to use the lexical form everywhere.. that way I don't have to parse by type.. although when showing results to user we'll have to customize their displays according to the type..
        //literal.getLiteralValue();
        //TODO using the lexical form won't work when we want to apply math or string functions to values in AQL!
        filterConditions.add(new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(forLoopVarName, ArangoAttributes.OBJECT, ArangoAttributes.VALUE)), new Const_String(literal.getLiteralLexicalForm())));
    }

    public static ExprList ProcessBindingsTableJoin(Table table, String forLoopVarName){
        //the FILTER commands should be added if there is a JOIN clause between a Bindings table and some other op..
        //thus the functionality below could be changed to represent the Table in Arango (ex. array of objects with the bound vars..)
        com.aql.algebra.expressions.Expr jointFilterExpr = null;
        List<Var> vars = table.getVars();
        for (Iterator<Binding> i = table.rows(); i.hasNext();){
            com.aql.algebra.expressions.Expr currExpr = ProcessBinding(i.next(), vars, forLoopVarName);

            if(jointFilterExpr == null){
                jointFilterExpr = currExpr;
            }
            else{
                jointFilterExpr = new Expr_LogicalOr(jointFilterExpr, currExpr);
            }
        }
        return new ExprList(jointFilterExpr);
    }

    public static com.aql.algebra.expressions.Expr ProcessBinding(Binding binding, List<Var> vars, String forLoopVarName){
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
                currExpr = new Expr_Equals(com.aql.algebra.expressions.Var.alloc(AqlUtils.buildVar(forLoopVarName,var.getName())), new Const_String(value.toString()));
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

}

