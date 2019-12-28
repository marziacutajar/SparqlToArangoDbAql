package com.aql.querytree.expressions;

import com.aql.querytree.ExprVisitor;
import com.aql.querytree.operators.Op;

public abstract class Expr {

    public boolean isExpr()     { return true; }
    public final Expr getExpr() { return this; }

    //public final Set<Var> getVarsMentioned() { return ExprVars.getVarsMentioned(this); }

    //public final void varsMentioned(Collection<Var> acc) { ExprVars.varsMentioned(acc, this); }

    // ---- Default implementations
    public boolean isVariable()         { return false; }
    public String getVarName()          { return null; }
    public ExprVar getExprVar()         { return null; }
    public Var asVar()                  { return null; }

    public boolean isConstant()         { return false; }
    public Constant getConstant()       { return null; }
    public boolean isFunction()         { return false; }
    public ExprFunction getFunction()   { return null; }

    public boolean isSubquery()         { return false; }
    public Op getSubquery()             { return null; }

    public void visit(ExprVisitor visitor) {  }
}
