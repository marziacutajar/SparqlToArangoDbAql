package com.aql.algebra.expressions.functions;

import com.aql.algebra.expressions.Expr;

public class Expr_DateNow extends ExprFunction0 {
    private static final String functionName = "date_now";

    public Expr_DateNow(){
        super(functionName);
    }

    @Override
    public Expr copy() {  return new Expr_DateNow(); }

}
