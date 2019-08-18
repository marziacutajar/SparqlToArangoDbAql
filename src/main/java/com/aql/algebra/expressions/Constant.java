package com.aql.algebra.expressions;

import com.sparql_to_aql.exceptions.AqlExprTypeException;

public abstract class Constant extends Expr
{
    // Don't create direct - the static builders manage the value/node relationship
    protected Constant() { super(); }

    @Override
    public boolean isConstant() { return true; }

    @Override
    public Constant getConstant()     { return this; }

    // ----------------------------------------------------------------
    // ---- Subclass operations

    public boolean isBoolean()      { return false; }
    public boolean isString()       { return false; }
    public boolean isNumber()       { return false; }

    public boolean getBoolean() throws AqlExprTypeException {
        raise(new AqlExprTypeException("Not a boolean: "+this)); return false;
    }

    public String  getString() throws AqlExprTypeException {
        raise(new AqlExprTypeException("Not a string: "+this)); return null;
    }
    public double getNumber() throws AqlExprTypeException {
        raise(new AqlExprTypeException("Not a double: "+this)); return Double.NaN;
    }

    //public Expr apply(ExprTransform transform)  { return transform.transform(this); }

    // Point to catch all exceptions.
    public static void raise(AqlExprTypeException ex) throws AqlExprTypeException
    {
        throw ex;
    }

    /*@Override
    public String toString()
    {
        return asQuotedString();
    }

    public final String asQuotedString()
    { return asQuotedString(new SerializationContext()); }

    public final String asQuotedString(SerializationContext context)
    {
        // If possible, make a node and use that as the formatted output.
        if ( node == null )
            node = asNode();
        if ( node != null )
            return FmtUtils.stringForNode(node, context);
        return toString();
    }
     */
}
