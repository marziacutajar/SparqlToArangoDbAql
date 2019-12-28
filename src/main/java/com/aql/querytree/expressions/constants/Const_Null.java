package com.aql.querytree.expressions.constants;

import com.aql.querytree.expressions.Constant;

public class Const_Null extends Constant {

    @Override
    public boolean isNull() { return true; }

    @Override
    public String toString(){
        return "null";
    }

}
