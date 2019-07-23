package com.sparql_to_aql;

//class used for rewriting of SPARQL algebra expression to AQL
//TODO decide whether we will "rewrite" the expression to use algebra operators that are
//more AQL specific (by creating custom Op and sub operators for AQL), or whether we will
//translate the SPARQL algebra expressions directly to an AQL query (would be hard to re-optimise such a query though..)

import com.sparql_to_aql.entities.aql.algebra.AqlOp;
import org.apache.jena.query.SortCondition;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.expr.Expr;

import java.util.List;
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

    @Override
    public void visit(OpBGP opBpg){
    }

    @Override
    public void visit(OpTriple opTriple){
    }

    @Override
    public void visit(OpQuad opQuad){
    }

    @Override
    public void visit(OpJoin opJoin){
    }

    @Override
    public void visit(OpLeftJoin opLeftJoin){
    }

    @Override
    public void visit(OpMinus opMinus){
    }

    @Override
    public void visit(OpFilter opFilter){
    }

    @Override
    public void visit(OpExtend opExtend){
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
