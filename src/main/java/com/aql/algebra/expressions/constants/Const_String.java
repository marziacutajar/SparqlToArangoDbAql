package com.aql.algebra.expressions.constants;

import com.aql.algebra.expressions.Constant;

public class Const_String extends Constant {
    private String string;

    public Const_String(String str)         { string = str; }

    @Override
    public boolean isString() { return true; }

    @Override
    public String getString() { return string; }

    @Override
    public String toString()
    {
        return '"'+string+'"';
    }
}
