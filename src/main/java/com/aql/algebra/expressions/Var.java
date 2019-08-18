package com.sparql_to_aql.entities.algebra.aql.expressions;

public class Var extends Expr
{
    final protected String name;

    public static Var alloc(String varName) {
        return new Var(varName);
    }

    public static Var alloc(Var v) {
        return v;
    }

    public static Var alloc(ExprVar nv)         { return new Var(nv); }

    // Precalulated the hash code because hashCode() is used so heavily with Var's
    //private final int hashCodeValue;

    private Var(String varName){
        name = varName;
        //hashCodeValue = super.hashCode();
    }

    private Var(ExprVar v)           { this(v.getVarName()); }

    @Override
    public boolean isVariable() {
        return true;
    }

    @Override
    public String getVarName() {
        return name;
    }

    //@Override
    //public final int hashCode() { return hashCodeValue; }

    @Override
    public final boolean equals(Object other) {
        if ( this == other ) return true;
        if ( ! ( other instanceof Var ) ) return false;
        return super.equals(other);
    }

    // -------

    public static String canonical(String x) {
        if ( x.startsWith("?") )
            return x.substring(1);
        if ( x.startsWith("$") )
            return x.substring(1);
        return x;
    }

    public static boolean isVariable(Expr expr) {
        if ( expr instanceof Var )
            return true;
        if ( expr != null && expr.isVariable() )
            throw new RuntimeException("Invalid variable found");
        return false;
    }
}

