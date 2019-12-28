package com.aql.querytree.expressions.functions;

import com.aql.querytree.AqlConstants;
import com.aql.querytree.expressions.Expr;

public class Expr_LessThan extends ExprFunction2 {
    private static final String functionName = "lt";
    private static final String symbol = AqlConstants.SYM_LT;

    public Expr_LessThan(Expr left, Expr right)
    {
        super(left, right, functionName, symbol);
    }

    @Override
    public Expr copy(Expr e1, Expr e2) {  return new Expr_LessThan(e1 , e2); }

}
