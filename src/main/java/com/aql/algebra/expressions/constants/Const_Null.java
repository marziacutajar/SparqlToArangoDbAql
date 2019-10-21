package com.aql.algebra.expressions.constants;

import com.aql.algebra.expressions.Constant;

public class Const_Null extends Constant {
    @Override
    public boolean isNull()  { return true; }

    @Override
    public String toString(){
        return "null";
    }

}
