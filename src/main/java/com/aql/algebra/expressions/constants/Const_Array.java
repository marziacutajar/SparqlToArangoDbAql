package com.aql.algebra.expressions.constants;

import com.aql.algebra.expressions.Constant;

public class Const_Array extends Constant {
    private Constant[] array;

    public Const_Array(Constant[] array){
        this.array = array;
    }
}
