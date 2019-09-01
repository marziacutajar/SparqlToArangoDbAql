package com.aql.algebra;

import com.aql.algebra.expressions.Expr;
import com.aql.algebra.expressions.ExprVar;
import com.aql.algebra.expressions.Var;

public class SortCondition {
    public Expr expression = null;
    public Direction direction;

    public enum Direction {
        ASC,
        DESC
    }

    public SortCondition(Var var, Direction dir)
    { this(new ExprVar(var), dir); }

    public SortCondition(Expr expr, Direction dir)
    {
        expression = expr ;
        direction = dir ;
    }

    /** @return Returns the direction. */
    public Direction getDirection()
    {
        return direction;
    }

    /** @return Returns the expression. */
    public Expr getExpression()
    {
        return expression ;
    }

    /*@Override
    public int hashCode()
    {
        int x = this.getDirection() ;
        if ( getExpression() != null )
            x ^= getExpression().hashCode() ;
        return x ;
    }

    @Override
    public boolean equals(Object other)
    {
        if ( this == other ) return true ;

        if ( ! ( other instanceof SortCondition ) )
            return false ;

        SortCondition sc = (SortCondition)other ;

        if ( sc.getDirection() != this.getDirection() )
            return false ;

        if ( ! Objects.equals(this.getExpression(), sc.getExpression()) )
            return false ;

//        if ( ! Utils.eq(this.getVariable(), sc.getVariable()) )
//            return false ;

        return true ;
    }

    @Override
    public void output(IndentedWriter out)
    {
        out.print(Plan.startMarker) ;
        out.print("SortCondition ") ;
        FmtExprSPARQL fmt = new FmtExprSPARQL(out, null) ;
        format(fmt, out) ;
        out.print(Plan.finishMarker) ;
    }

    @Override
    public void output(IndentedWriter out, SerializationContext sCxt)
    {
        if ( sCxt == null )
            sCxt = new SerializationContext() ;
        FmtExprSPARQL fmt = new FmtExprSPARQL(out, sCxt) ;
        format(fmt, out) ;
    }*/
}
