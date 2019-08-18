package com.sparql_to_aql.entities.algebra.aql.expressions.functions;

import com.sparql_to_aql.entities.algebra.aql.AqlConstants;
import com.sparql_to_aql.entities.algebra.aql.expressions.Expr;

public class Expr_Length extends ExprFunction1{
    private static final String symbol = AqlConstants.SYM_AND;

    public Expr_Length(Expr expr)
    {
        super(expr, symbol);
    }

    @Override
    public Expr copy(Expr expr) {  return new Expr_Length(expr); }

}
