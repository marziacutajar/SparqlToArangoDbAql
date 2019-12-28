package com.aql.querytree.expressions.constants;

import com.aql.querytree.expressions.Constant;

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

    @Override
    public String toString(){
        String val = "[";
        for (int i = 0; i < array.length; i++) {
            val += array[i].toString();
            if(i < array.length - 1)
                val += ", ";
        }
        val += "]";
        return val;
    }
}
