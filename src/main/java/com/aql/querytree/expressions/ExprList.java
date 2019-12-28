package com.aql.querytree.expressions;

import java.util.*;

public class ExprList implements Iterable<Expr> {
    private final List<Expr> expressions;

    /** Create a copy which does not share the list of expressions with the original */
    public static ExprList copy(ExprList other) { return new ExprList(other); }

    /** Create an ExprList that contains the expressions */
    public static ExprList create(Collection<Expr> exprs) {
        ExprList exprList = new ExprList();
        exprs.forEach(exprList::add);
        return exprList;
    }

    /** Empty, immutable ExprList */
    public static final ExprList emptyList = new ExprList(Collections.emptyList());

    public ExprList() { expressions = new ArrayList<>(); }

    private ExprList(ExprList other) {
        this();
        expressions.addAll(other.expressions);
    }

    public ExprList(Expr expr) {
        this();
        expressions.add(expr);
    }

    public ExprList(List<Expr> x)   { expressions = x; }

    public Expr get(int idx)                            { return expressions.get(idx); }
    public int size()                                   { return expressions.size(); }
    public boolean isEmpty()                            { return expressions.isEmpty(); }
    public ExprList subList(int fromIdx, int toIdx)     { return new ExprList(expressions.subList(fromIdx, toIdx)); }
    public ExprList tail(int fromIdx)                   { return subList(fromIdx, expressions.size()); }

    /*public Set<Var> getVarsMentioned() {
        Set<Var> x = new HashSet<>();
        varsMentioned(x);
        return x;
    }*/

    public void addAll(ExprList exprs)      { expressions.addAll(exprs.getList()); }
    public void add(Expr expr)              { expressions.add(expr); }
    public List<Expr> getList()             { return Collections.unmodifiableList(expressions); }
    public List<Expr> getListRaw()          { return expressions; }
    @Override
    public Iterator<Expr> iterator()        { return expressions.iterator(); }

    @Override
    public String toString()
    { return expressions.toString(); }

    /*public static ExprList splitConjunction(ExprList exprList1) {
        ExprList exprList2 = new ExprList();
        for (Expr expr : exprList1)
            split(exprList2, expr);
        return exprList2;
    }

    private static ExprList splitConjunction(Expr expr) {
        ExprList exprList = new ExprList();
        split(exprList, expr);
        return exprList;
    }

    private static void split(ExprList exprList, Expr expr) {
        // Explode &&-chain to exprlist.
        while (expr instanceof E_LogicalAnd) {
            E_LogicalAnd x = (E_LogicalAnd)expr;
            Expr left = x.getArg1();
            Expr right = x.getArg2();
            split(exprList, left);
            expr = right;
        }
        // Drop through and add remaining
        exprList.add(expr);
    }*/
}
