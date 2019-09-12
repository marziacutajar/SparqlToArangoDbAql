package com.aql.algebra.expressions.constants;

import com.aql.algebra.expressions.Constant;
import com.aql.algebra.expressions.VarExprList;

public class Const_Object extends Constant {
    private VarExprList keyValues;

    public Const_Object(){
        this.keyValues = new VarExprList();
    }

    public Const_Object(VarExprList keyValues){
        this.keyValues = keyValues;
    }

    @Override
    public boolean isObject() { return true; }

    @Override
    public VarExprList getObject()  { return keyValues; }
}
