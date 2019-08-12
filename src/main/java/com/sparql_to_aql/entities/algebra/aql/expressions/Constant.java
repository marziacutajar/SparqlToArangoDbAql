package com.sparql_to_aql.entities.algebra.aql.expressions;

public class Constant extends Expr
{
    //TODO can be boolean, string, number, array, bla bla.. try to represent this

    @Override
    public boolean isConstant()        { return true; }

    //TODO change value returned below
    @Override
    public Constant getConstant()      { return null; }
}
