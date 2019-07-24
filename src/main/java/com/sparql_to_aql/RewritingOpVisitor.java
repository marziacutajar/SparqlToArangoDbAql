package com.sparql_to_aql;

//class used for rewriting of SPARQL algebra expression to AQL
//TODO decide whether we will "rewrite" the expression to use algebra operators that are
//more AQL specific (by creating custom Op and sub operators for AQL), or whether we will
//translate the SPARQL algebra expressions directly to an AQL query (would be hard to re-optimise such a query though..)

import com.sparql_to_aql.constants.ArangoDatabaseSettings;
import com.sparql_to_aql.constants.NodeRole;
import com.sparql_to_aql.entities.aql.algebra.AqlOp;
import com.sparql_to_aql.utils.RewritingUtils;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.SortCondition;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.expr.Expr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

//TODO If rewriting to the actual AQL query, use StringBuilder (refer to https://www.codeproject.com/Articles/1241363/Expression-Tree-Traversal-Via-Visitor-Pattern-in-P)
public class RewritingOpVisitor extends RewritingOpVisitorBase {

    //TODO build Aql query expression tree using below if we're gonna have seperate AQL algebra structure
    private AqlOp _aqlAlgebraQueryExpression;

    //This method is to be called after the visitor has been used
    public AqlOp GetAqlAlgebraQueryExpression()
    {
        return _aqlAlgebraQueryExpression;
    }

    private int forLoopCounter = 0;
    //Keep track if which variables have already been bound in outer for loops
    private static Map<String, List<String>> usedVariablesByForLoopItem = new HashMap<>();

    //TODO used for keeping record of created and used for loop vars
    //private List<String> forLoopVars = new ArrayList<>();

    @Override
    public void visit(OpBGP opBpg){
        System.out.println("Entering opBgp");
        for(Triple triple : opBpg.getPattern().getList()){
            //TODO possibly copy below logic to visit(OpTriple) and then call that method in here..
            List<String> usedVars = new ArrayList<>();
            List<String> filterConditions = new ArrayList<>();
            forLoopCounter++;
            String forloopvarname = "forloop" + forLoopCounter + "item";

            System.out.println("FOR " + forloopvarname + " IN " + ArangoDatabaseSettings.rdfCollectionName);

            //TODO also keep list of filter clauses per triple..
            RewritingUtils.ProcessTripleNode(triple.getSubject(), NodeRole.SUBJECT, forloopvarname, usedVars, filterConditions);
            RewritingUtils.ProcessTripleNode(triple.getPredicate(), NodeRole.PREDICATE, forloopvarname, usedVars, filterConditions);
            RewritingUtils.ProcessTripleNode(triple.getObject(), NodeRole.OBJECT, forloopvarname, usedVars, filterConditions);
            //TODO create AQL subqueries? not sure if this is needed here or in visit(OpTriple)
            usedVariablesByForLoopItem.put(forloopvarname, usedVars);
        }
    }

    @Override
    public void visit(OpTriple opTriple){
        System.out.println("TRIPLE HERE");
    }

    @Override
    public void visit(OpQuad opQuad){
    }

    @Override
    public void visit(OpJoin opJoin){
        System.out.println("Entering join");
        //TODO Here we'll need to use some current list of common variables between the resulting "bgps" that must be joined
        //and create a FILTER statement with them
    }

    @Override
    public void visit(OpLeftJoin opLeftJoin){
    }

    @Override
    public void visit(OpMinus opMinus){
    }

    @Override
    public void visit(OpFilter opFilter){
        opFilter.getExprs();
        //TODO iterate over expressions, add filter conditions in AQL format to some List<String> for concatenating later
    }

    @Override
    public void visit(OpExtend opExtend){
        opExtend.getVarExprList();
        //TODO loop over each var in the list and for each of them create clause LET varName = value of expr for var
    }

    @Override
    public void visit(OpUnion opUnion){
    }

    @Override
    public void visit(OpGraph opGraph){
    }

    @Override
    public void visit(OpProject opProject){
        String delimitedVariables = opProject.getVars().stream().map(v -> v.getVarName())
                .collect( Collectors.joining( ", " ) );
        System.out.println("RETURN " + delimitedVariables);
    }

    @Override
    public void visit(OpOrder opOrder) {
        List<SortCondition> sortConditionList = opOrder.getConditions();
        String[] conds = new String[sortConditionList.size()];

        for (int i= 0; i < sortConditionList.size(); i++) {
            SortCondition currCond = sortConditionList.get(i);
            //direction = 1 if ASC, -1 if DESC, -2 if unspecified (default asc)
            String direction = currCond.getDirection() == -1 ? "DESC" : "ASC";
            conds[i] = currCond.getExpression() + " " + direction;
        }

        System.out.println("SORT " + String.join(", ", conds));
    }

    @Override
    public void visit(OpDistinct opDistinct){
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

    @Override
    public void visit(OpTopN opTopN){
        //TODO this is only present if we use TransformTopN optimizer to change from OpSlice to OpTopN..
        //This operator contains limit + order by for better performance
        //https://jena.apache.org/documentation/javadoc/arq/org/apache/jena/sparql/algebra/op/OpTopN.html

        for(SortCondition cond : opTopN.getConditions()){
            int direction = cond.getDirection();
            Expr expr = cond.getExpression();
            //TODO use above
        }
        System.out.println("LIMIT " + opTopN.getLimit());
    }

    //TODO decide whether to add visit methods for OpList, OpPath and others.. or whether they'll be unsupported

}
