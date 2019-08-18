package com.sparql_to_aql.entities.algebra.aql.expressions;

import java.util.ArrayList;
import java.util.List;

public abstract class ExprFunction extends Expr {
    //protected FunctionLabel funcSymbol;
    protected String funcName;
    protected String opSign;

    protected ExprFunction(String fName) {
        //funcSymbol = new FunctionLabel(fName);
        funcName = fName;
        opSign = null;
    }

    protected ExprFunction(String fName, String opSign)
    {
        this(fName);
        this.opSign = opSign;
    }

    public abstract Expr getArg(int i);
    public abstract int numArgs();

    // ExprFunctionN overrides this.
    public List<Expr> getArgs() {
        List<Expr> argList = new ArrayList<>(numArgs());
        for ( int i = 1; i <= numArgs(); i++ )
            argList.add(this.getArg(i));
        return argList;
    }

    @Override
    public boolean isFunction()        { return true; }

    @Override
    public ExprFunction getFunction()  { return this; }

    /*@Override
    public int hashCode() {
        return funcSymbol.hashCode() ^ numArgs();
    }*/

    // A function is equal if:
    // + The name is the same
    // + The arguments are the same (including arity).

    /*@Override
    public boolean equals(Expr other, boolean bySyntax) {
        if ( other == null ) return false;
        if ( this == other ) return true;

        if ( ! other.getClass().equals(this.getClass()) )
            return false;

        ExprFunction ex = (ExprFunction)other;

        if ( ! funcSymbol.equals(ex.funcSymbol) )
            return false;

        if ( numArgs() != ex.numArgs() )
            return false;

        // Arguments are 1, 2, 3, ...
        for ( int i = 1; i <= numArgs(); i++ )
        {
            Expr a1 = this.getArg(i);
            Expr a2 = ex.getArg(i);
            if ( ! a1.equals(a2, bySyntax) )
                return false;
        }
        return true;
    }*/

    /** Name used for output in SPARQL format needing functional form (no specific keyword).
     *  e.g. regexp(), custom functions, ...
     */

    /*public String getFunctionPrintName(SerializationContext cxt)
    { return funcSymbol.getSymbol(); }*/

    /** Name used in a functional form (i.e. SPARQL algebra).
     *  getOpName() is used in preference as a short, symbol name.
     */
    /*public String getFunctionName(SerializationContext cxt)
    { return funcSymbol.getSymbol(); }*/

    /** Used to get a unique name for the function, which is intern'ed.  Used in hashCode() */
    /*public FunctionLabel getFunctionSymbol()
    { return funcSymbol; }*/
    public String getFunctionName()
    { return funcName; }

    /** Get the symbol name (+, ! etc) for this function -- maybe null for none */
    public String getOpName()
    { return opSign; }
}
