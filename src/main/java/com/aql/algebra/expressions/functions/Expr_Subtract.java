package com.sparql_to_aql.entities.algebra.aql.expressions.functions;

import com.sparql_to_aql.entities.algebra.aql.AqlConstants;
import com.sparql_to_aql.entities.algebra.aql.expressions.Expr;

public class Expr_Subtract extends ExprFunction2 {
    private static final String functionName = "subtract";
    private static final String symbol = AqlConstants.SYM_SUBTRACT;

    public Expr_Subtract(Expr left, Expr right)
    {
        super(left, right, functionName, symbol);
    }

    @Override
    public Expr copy(Expr e1, Expr e2) {  return new Expr_Subtract(e1 , e2); }

}
