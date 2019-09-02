package com.aql.algebra.expressions.constants;

import com.aql.algebra.expressions.Constant;

public class Const_Bool extends Constant {
    private boolean bool = false;

    public Const_Bool(boolean b)         { super();  bool = b; }

    @Override
    public boolean isBoolean()  { return true; }

    @Override
    public boolean getBoolean() { return bool; }

    @Override
    public String toString()
    { return bool ? "true" : "false"; }
}
