package com.sparql_to_aql.entities.algebra.aql.expressions.constants;

import com.sparql_to_aql.entities.algebra.aql.expressions.Constant;

public class Const_Number extends Constant {
    double value = Double.NaN;

    public Const_Number(double d)         { super(); value = d; }

    @Override
    public boolean isNumber() { return true; }

    @Override
    public double getNumber()  { return value; }

    /*@Override
    public String toString()
    {
        // Preserve lexical form
        if ( getNode() != null ) return super.asString();
        return Utils.stringForm(value);
    }*/
}
