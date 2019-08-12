package com.sparql_to_aql.entities.algebra.aql.expressions.functions;

import com.sparql_to_aql.entities.algebra.aql.AqlConstants;
import com.sparql_to_aql.entities.algebra.aql.expressions.Expr;

public class Expr_LogicalNot extends ExprFunction1 {
    private static final String functionName = "not";
    private static final String symbol = AqlConstants.SYM_NOT;

    public Expr_LogicalNot(Expr expr)
    {
        super(expr, functionName, symbol);
    }

    @Override
    public Expr copy(Expr expr) { return new Expr_LogicalNot(expr); }

}
