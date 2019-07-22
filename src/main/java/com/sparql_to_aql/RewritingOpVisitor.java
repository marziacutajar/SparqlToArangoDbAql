package com.sparql_to_aql;

//class used for rewriting of SPARQL algebra expression to AQL
//TODO decide whether we will "rewrite" the expression to use algebra operators that are
//more AQL specific (by creating custom Op and sub operators for AQL), or whether we will
//translate the SPARQL algebra expressions directly to an AQL query (would be hard to re-optimise such a query though..)

import com.sparql_to_aql.entities.aql.algebra.AqlOp;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;

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
    }

    @Override
    public void visit(OpOrder opOrder){
    }

    @Override
    public void visit(OpDistinct opDistinct){
    }

    @Override
    public void visit(OpReduced opReduced){
    }

    @Override
    public void visit(OpSlice opSlice){
    }

    @Override
    public void visit(OpTopN opTopN){
    }

    //TODO decide whether to add visit methods for OpList, OpPath and others.. or whether they'll be unsupported

}
