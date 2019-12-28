package com.aql.querytree.operators;

import com.aql.querytree.AqlConstants;
import com.aql.querytree.AqlQueryNode;
import com.aql.querytree.NodeVisitor;
import com.aql.querytree.expressions.ExprVar;
import com.aql.querytree.expressions.VarExprList;

public class OpCollect extends Op1
{
    private VarExprList varExprs;
    private boolean withCount;
    private ExprVar countVar;

    public OpCollect(AqlQueryNode subOp, VarExprList groupVars)
    {
        super(subOp);
        this.varExprs  = groupVars;
        this.withCount = false;
    }

    public OpCollect(AqlQueryNode subOp, ExprVar countVar)
    {
        super(subOp);
        this.varExprs  = new VarExprList();
        this.withCount = true;
        this.countVar = countVar;
    }

    @Override
    public String getName()                     { return AqlConstants.keywordCollect; }
    public VarExprList getVarExprs()           { return varExprs; }

    public boolean isWithCount(){
        return withCount;
    }

    public ExprVar getCountVar(){
        if(withCount)
            return countVar;
        else return null;
    }

    @Override
    public void visit(NodeVisitor opVisitor){
        opVisitor.visit(this);
    }

    @Override
    public Op1 copy(AqlQueryNode child){
        return new OpCollect(child, varExprs);
    }
}
