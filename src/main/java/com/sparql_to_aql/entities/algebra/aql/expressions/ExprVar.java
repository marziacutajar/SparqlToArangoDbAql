package com.sparql_to_aql.entities.algebra.aql;

public class ExprVar {
    protected Var var = null;

    public ExprVar(String name) { var = Var.alloc(name) ; }

    public ExprVar(Var v)
    {
        var = v ;
    }

    //public Expr copy(Var v)  { return new ExprVar(v) ; }

    /*@Override
    public void visit(ExprVisitor visitor) { visitor.visit(this) ; }

    public Expr apply(ExprTransform transform)  {
        if ( transform == null )
            throw new NullPointerException() ;
        return transform.transform(this) ; }

    public void format(Query query, IndentedWriter out)
    {
        out.print('?') ;
        out.print(varNode.getName()) ;
    }

    @Override
    public int hashCode() { return varNode.hashCode() ; }

    @Override
    public boolean equals(Expr other, boolean bySyntax) {
        if ( other == null ) return false ;
        if ( this == other ) return true ;
        if ( ! ( other instanceof ExprVar ) )
            return false ;
        ExprVar nvar = (ExprVar)other ;
        return getVarName().equals(nvar.getVarName()) ;
    }*/

    /** @return Returns the name. */
    //@Override
    public String getVarName()  { return var.getName() ; }

    //@Override
    public ExprVar getExprVar() { return this ; }

    @Override
    public String toString()        { return var.toString() ; }
}
