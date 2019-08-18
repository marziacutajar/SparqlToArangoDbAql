package com.aql.algebra.expressions;

import com.aql.algebra.Aggregator;

public class ExprAggregator extends Expr {

    protected Aggregator aggregator;
    protected Var var;
    protected ExprVar exprVar = null;

    public ExprAggregator(Var v, Aggregator agg)          { _setVar(v); aggregator = agg; }
    public Var getVar()                                 { return var; }

    /*public void setVar(Var v)
    {
        if (this.var != null)
            throw new ARQInternalErrorException(Lib.className(this)+ ": Attempt to set variable to " + v + " when already set as " + this.var);
        if (v == null)
            throw new ARQInternalErrorException(Lib.className(this)+ ": Attempt to set variable to null");
        _setVar(v);
    }*/

    private void _setVar(Var v)
    {
        this.var = v;
        this.exprVar = new ExprVar(var);
    }

    public Aggregator getAggregator()   { return aggregator; }

    /*@Override
    public int hashCode()
    {
        int x = aggregator.hashCode();
        if ( var != null )
            x ^= var.hashCode();
        return x;
    }

    @Override
    public boolean equals(Expr other, boolean bySyntax) {
        if ( other == null ) return false;
        if ( this == other ) return true;
        if ( ! ( other instanceof ExprAggregator ) )
            return false;
        ExprAggregator agg = (ExprAggregator)other;
        if ( ! Objects.equals(var, agg.var) )
            return false;
        return Objects.equals(aggregator, agg.aggregator);
    }

    // Ensure no confusion - in an old design, an ExprAggregator was a subclass of ExprVar.
    @Override
    public ExprVar getExprVar()
    { throw new ARQInternalErrorException(); }

    @Override
    public Var asVar()
    { throw new ARQInternalErrorException(); } */

    public ExprVar getAggVar() { return exprVar; }

    // As an expression suitable for outputting the calculation.
    /*public String asSparqlExpr(SerializationContext sCxt)
    { return aggregator.asSparqlExpr(sCxt); }

    @Override
    public ExprAggregator copySubstitute(Binding binding)
    {
        Var v = var;
        Aggregator agg = aggregator;
        return new ExprAggregator(v, agg);
    }

    @Override
    public ExprAggregator applyNodeTransform(NodeTransform transform)
    {
        // Can't rewrite this to a non-variable.
        Node node = transform.apply(var);
        if ( ! Var.isVar(node) )
        {
            Log.warn(this, "Attempt to convert an aggregation variable to a non-variable: ignored");
            node = var;
        }

        Var v = (Var)node;
        Aggregator agg = aggregator.copyTransform(transform);
        return new ExprAggregator(Var.alloc(node), agg);
    }*/

    // DEBUGGING
    /*@Override
    public String toString()
    { return "(AGGREGATE "+
            (var==null?"<>":"?"+var.getVarName())+
            " "+aggregator.toString()+")"; }

    public Expr copy(Var v)  { return new ExprAggregator(v, aggregator.copy(aggregator.getExprList())); }

    @Override
    public NodeValue eval(Binding binding, FunctionEnv env)
    {
        return ExprVar.eval(var, binding, env);
    }

    public Expr apply(ExprTransform transform)  { return transform.transform(this); }

    @Override
    public void visit(ExprVisitor visitor)
    { visitor.visit(this); } */
}
