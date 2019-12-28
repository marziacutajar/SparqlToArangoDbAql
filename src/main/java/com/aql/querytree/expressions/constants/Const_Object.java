package com.aql.querytree.expressions.constants;

import com.aql.querytree.expressions.Constant;
import com.aql.querytree.expressions.Expr;
import java.util.HashMap;
import java.util.Iterator;
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
    public boolean isObject(){
        return true;
    }

    @Override
    public Map<String, Expr> getObject(){
        return keyValues;
    }

    @Override
    public String toString(){
        String val = "{";
        Iterator<Map.Entry<String, Expr>> it = keyValues.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Expr> pair = it.next();
            val += pair.getKey() + ": ";
            val += pair.getValue().toString();
            if(it.hasNext()){
                val += ", ";
            }
        }
        val += "}";
        return val;
    }
}
