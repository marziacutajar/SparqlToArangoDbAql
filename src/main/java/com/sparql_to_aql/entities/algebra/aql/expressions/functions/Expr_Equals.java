package com.sparql_to_aql.entities.algebra.aql.expressions.functions;

import com.sparql_to_aql.entities.algebra.aql.AqlConstants;
import com.sparql_to_aql.entities.algebra.aql.expressions.Expr;

public class Expr_Equals extends ExprFunction2{
    private static final String functionName = "eq";
    private static final String symbol = AqlConstants.SYM_EQ;

    public Expr_Equals(Expr left, Expr right)
    {
        super(left, right, functionName, symbol);
    }

    @Override
    public Expr copy(Expr e1, Expr e2) {  return new Expr_Equals(e1 , e2); }
}
