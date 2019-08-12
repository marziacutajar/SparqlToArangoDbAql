package com.sparql_to_aql.entities.algebra.aql.expressions.functions;

import com.sparql_to_aql.entities.algebra.aql.expressions.Expr;
import com.sparql_to_aql.entities.algebra.aql.expressions.ExprFunction;
import com.sparql_to_aql.entities.algebra.aql.expressions.ExprList;

import java.util.List;

/** A function which takes N arguments (N may be variable e.g. regex) */
public abstract class ExprFunctionN extends ExprFunction {

    protected ExprList args = null;

    protected ExprFunctionN(String fName, Expr... args)
    {
        this(fName, argList(args));
    }

    protected ExprFunctionN(String fName, ExprList args)
    {
        super(fName);
        this.args = args;
    }

    private static ExprList argList(Expr[] args)
    {
        ExprList exprList = new ExprList();
        for ( Expr e : args )
            if ( e != null )
                exprList.add(e);
        return exprList;
    }

    @Override
    public Expr getArg(int i)
    {
        i = i-1;
        if ( i >= args.size() )
            return null;
        return args.get(i);
    }

    @Override
    public int numArgs() { return args.size(); }

    @Override
    public List<Expr> getArgs() { return args.getList(); }

    public abstract Expr copy(ExprList newArgs);

    /*@Override
    public void visit(ExprVisitor visitor) { visitor.visit(this); }
    public Expr apply(ExprTransform transform, ExprList exprList) { return transform.transform(this, exprList); }*/

}
