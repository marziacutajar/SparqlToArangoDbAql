package com.aql.algebra.operators;

import com.aql.algebra.AqlConstants;
import com.aql.algebra.OpVisitor;
import com.aql.algebra.expressions.Expr;

import java.util.ArrayList;
import java.util.List;

public class OpProject extends OpModifier {

    private List<Expr> exprs = new ArrayList<>();

    private boolean distinct;

    public OpProject(Op subOp, List<Expr> exprs, boolean distinct)
    {
        super(subOp);
        this.exprs = exprs;
        this.distinct = distinct;
    }

    public OpProject(Op subOp, Expr expr, boolean distinct)
    {
        super(subOp);
        this.exprs.add(expr);
        this.distinct = distinct;
    }

    public List<Expr> getExprs() { return exprs; }

    @Override
    public String getName() { return AqlConstants.keywordReturn; }
    @Override
    public void visit(OpVisitor opVisitor)  { opVisitor.visit(this); }
    @Override
    public Op1 copy(Op subOp)                { return new OpProject(subOp, exprs, distinct); }

    /*@Override
    public Op apply(Transform transform, Op subOp)
    { return transform.transform(this, subOp); }*/

    /*@Override
    public int hashCode()
    {
        return exprs.hashCode() ^ getSubOp().hashCode();
    }*/

    /*@Override
    public boolean equalTo(Op other, NodeIsomorphismMap labelMap)
    {
        if ( ! (other instanceof OpProject) ) return false;
        OpProject opProject = (OpProject)other;
        if ( ! Objects.equals(vars, opProject.vars ) )
            return false;
        return getSubOp().equalTo(opProject.getSubOp(), labelMap);
    }*/
}
