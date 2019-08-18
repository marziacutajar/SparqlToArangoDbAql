package com.sparql_to_aql.entities.algebra.aql.expressions.constants;

import com.sparql_to_aql.entities.algebra.aql.expressions.Constant;

public class Const_Bool extends Constant {
    boolean bool = false;

    public Const_Bool(boolean b)         { super();  bool = b; }

    @Override
    public boolean isBoolean()  { return true; }

    @Override
    public boolean getBoolean() { return bool; }

    @Override
    public String toString()
    { return bool ? "true" : "false"; }
}
