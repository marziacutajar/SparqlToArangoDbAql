package com.aql.algebra.expressions.constants;

import com.aql.algebra.expressions.Constant;

public class Const_Array extends Constant {
    private Constant[] array;

    public Const_Array(Constant[] array){
        this.array = array;
    }

    public Const_Array(Constant constant){
        this.array = new Constant[] {constant};
    }

    public Const_Array(){
        this.array = new Constant[0];
    }

    @Override
    public boolean isArray()  { return true; }

    @Override
    public Constant[] getArray()  { return array; }
}
