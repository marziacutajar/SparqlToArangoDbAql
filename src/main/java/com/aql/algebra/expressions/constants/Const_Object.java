package com.aql.algebra.expressions.constants;

import com.aql.algebra.expressions.Constant;
import com.aql.algebra.expressions.Expr;
import com.aql.algebra.expressions.VarExprList;

import java.util.HashMap;
import java.util.Map;

public class Const_Object extends Constant {
    Map<String, Expr> keyValues;

    public Const_Object(){
        this.keyValues = new HashMap<>();
    }

    public Const_Object(Map<String, Expr> keyValues){
        this.keyValues = keyValues;
    }

    @Override
    public boolean isObject() { return true; }

    @Override
    public Map<String, Expr> getObject()  { return keyValues; }
}
