package com.sparql_to_aql.entities.algebra.aql;

public class Constant extends Expr
{
    //TODO can be boolean, string, number, array, bla bla.. try to represent this

    @Override
    public boolean isConstant()        { return true; }

    @Override
    public Constant getConstant()      { return null ; }
}
