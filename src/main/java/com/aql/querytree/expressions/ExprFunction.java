package com.aql.querytree.expressions;

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

    public String getFunctionName()
    { return funcName; }

    /** Get the symbol name (+, ! etc) for this function -- maybe null for none */
    public String getOpName()
    { return opSign; }
}
