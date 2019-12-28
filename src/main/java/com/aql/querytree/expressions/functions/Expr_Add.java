package com.aql.querytree.expressions.functions;

import com.aql.querytree.AqlConstants;
import com.aql.querytree.expressions.Expr;

public class Expr_Add extends ExprFunction2 {
    private static final String functionName = "add";
    private static final String symbol = AqlConstants.SYM_PLUS;

    public Expr_Add(Expr left, Expr right)
    {
        super(left, right, functionName, symbol);
    }

    @Override
    public Expr copy(Expr e1, Expr e2) {  return new Expr_Add(e1 , e2); }
}
