package com.sparql_to_aql.entities.algebra.aql;

import java.util.Collection;
import java.util.Set;

public abstract class Expr {
    /*public static final Expr NONE = ExprNone.NONE0 ;

    public static final int CMP_GREATER  =  DatatypeConstants.GREATER ;
    public static final int CMP_EQUAL    =  DatatypeConstants.EQUAL ;
    public static final int CMP_LESS     =  DatatypeConstants.LESSER ;

    public static final int CMP_UNEQUAL  = -9 ;
    public static final int CMP_INDETERMINATE  = DatatypeConstants.INDETERMINATE ;*/

    public boolean isExpr()     { return true ; }
    public final Expr getExpr() { return this ; }

    //public final Set<Var> getVarsMentioned()                    { return ExprVars.getVarsMentioned(this) ; }

    //public final void varsMentioned(Collection<Var> acc)        { ExprVars.varsMentioned(acc, this) ; }

    // ---- Default implementations
    public boolean isVariable()         { return false ; }
    public String getVarName()          { return null ; }
    public ExprVar getExprVar()         { return null ; }
    public Var asVar()                  { return null ; }

    public boolean isConstant()         { return false ; }
    public Constant getConstant()      { return null ; }
    public boolean isFunction()         { return false ; }
    public ExprFunction getFunction()   { return null ; }

    //public String toString()            { return WriterExpr.asString(this) ; }
    //public void visit(ExprVisitor visitor) { visitor.visit(this) ; }
}
