package com.aql.algebra.expressions.constants;

import com.aql.algebra.expressions.Constant;

public class Const_Number extends Constant {
    private double value;

    public Const_Number(double d)         { super(); value = d; }

    @Override
    public boolean isNumber() { return true; }

    @Override
    public double getNumber()  { return value; }

    @Override
    public String toString()
    {
        return String.valueOf(value);
    }
}
