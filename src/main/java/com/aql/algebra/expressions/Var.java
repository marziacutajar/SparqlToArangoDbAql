package com.aql.algebra.expressions;

public class Var
{
    final protected String name;

    public static Var alloc(String varName) {
        return new Var(varName);
    }

    public static Var alloc(ExprVar nv) {
        return new Var(nv);
    }

    private Var(String varName){
        name = varName;
    }

    private Var(ExprVar v)           { this(v.getVarName()); }

    public boolean isVariable() {
        return true;
    }

    public String getVarName() {
        return name;
    }

    @Override
    public final boolean equals(Object other) {
        if ( this == other ) return true;
        if ( ! ( other instanceof Var ) ) return false;
        return super.equals(other);
    }

    public static boolean isVariable(Expr expr) {
        if (expr instanceof ExprVar)
            return true;

        return false;
    }
}

