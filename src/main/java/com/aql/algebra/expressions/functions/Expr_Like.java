package com.aql.algebra.expressions.functions;

import com.aql.algebra.expressions.Expr;
import com.aql.algebra.expressions.constants.Const_Bool;

//this is the LIKE function, not the LIKE keyword.. if we implement both, we must distinguish
public class Expr_Like extends ExprFunctionN //2 or 3 params
{
    private static final String functionName = "like";

    private final Expr text;
    private final Expr search;
    private final Expr caseInsensitive;

    public Expr_Like(Expr... args)
    {
        super(functionName, args);

        if(args.length < 2 || args.length > 3)
            throw new RuntimeException("Invalid number of parameters for LIKE function");

        this.text = args[0];
        this.search = args[1];

        if(args.length == 3){
            if(!(args[2] instanceof Const_Bool)) {
                throw new RuntimeException("Third parameter of the LIKE function must be a boolean value");
            }
            else{
                this.caseInsensitive = args[2];
            }
        }
        else{
            this.caseInsensitive = null;
        }
    }

    @Override
    public Expr copy(Expr... args)
    {
        return new Expr_Like(args);
    }
}
