package com.aql.algebra.operators;

import com.aql.algebra.AqlConstants;
import com.aql.algebra.AqlQueryNode;
import com.aql.algebra.NodeVisitor;
import com.aql.algebra.expressions.Expr;
import com.aql.algebra.expressions.ExprList;

public class OpFilter extends Op1
{
    protected ExprList expressions;

    /** Add expression - mutates an existing filter */
    public static Op filter(Expr expr, Op op) {
        OpFilter f = ensureFilter(op);
        f.getExprs().add(expr);
        return f;
    }

    /**
     * Ensure that the algebra op is a filter. If the input is a filter, just return that,
     * else create a filter with no expressions and "this" as the subOp.
     * @apiNote
     * This operation assumes the caller is going to add expressions.
     * Filters without any expressions are discouraged.
     * Consider collecting the expressions together first and using {@link #filterBy}.
     */
    public static OpFilter ensureFilter(Op op) {
        if (op instanceof OpFilter)
            return (OpFilter)op;
        else
            return new OpFilter(op);
    }

    /** Combine an ExprList with an Op so that the expressions filter the Op.
     * If the exprs are empty, return the Op.
     * If the op is already a OpFilter, merge the expressions into the filters existing expressions.
     * Else create a new OpFilter with the expressions and subOp.
     */
    public static Op filterBy(ExprList exprs, Op op) {
        if ( exprs == null || exprs.isEmpty() )
            return op;
        OpFilter f = ensureFilter(op);
        f.getExprs().addAll(exprs);
        return f;
    }

    /** Create a OpFilter with the expressions and subOp.
     * If subOp is a filter, combine expressions (de-layer).
     */
    public static OpFilter filterAlways(ExprList exprs, Op subOp) {
        OpFilter f = ensureFilter(subOp);
        f.getExprs().addAll(exprs);
        return f;
    }

    /** Make a OpFilter - guaranteed to return an fresh OpFilter */
    public static OpFilter filterDirect(ExprList exprs, Op op) {
        return new OpFilter(exprs, op);
    }

    /** Make a OpFilter - guaranteed to return an fresh OpFilter */
    public static OpFilter filterDirect(Expr expr, Op op) {
        OpFilter f = new OpFilter(op);
        f.getExprs().add(expr);
        return f;
    }

    public OpFilter(AqlQueryNode sub) {
        super(sub);
        expressions = new ExprList();
    }

    public OpFilter(ExprList exprs, AqlQueryNode sub) {
        super(sub);
        expressions = exprs;
    }

    public ExprList getExprs() { return expressions; }

    @Override
    public String getName() { return AqlConstants.keywordFilter; }

    /*@Override
    public Op apply(Transform transform, Op subOp)
    { return transform.transform(this, subOp); }*/

    @Override
    public void visit(NodeVisitor opVisitor) { opVisitor.visit(this); }

    @Override
    public Op1 copy(AqlQueryNode subOp)                { return new OpFilter(expressions, subOp); }
}
