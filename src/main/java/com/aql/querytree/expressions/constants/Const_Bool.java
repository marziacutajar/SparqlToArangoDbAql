package com.aql.querytree.expressions.constants;

import com.aql.querytree.expressions.Constant;

public class Const_Bool extends Constant {
    private boolean bool;

    public Const_Bool(boolean b){
        super();  bool = b;
    }

    @Override
    public boolean isBoolean() { return true; }

    @Override
    public boolean getBoolean() { return bool; }

    @Override
    public String toString(){
        return bool ? "true" : "false";
    }
}
