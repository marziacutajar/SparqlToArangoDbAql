package com.aql.algebra.operators;

import com.aql.algebra.AqlQueryNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class OpN implements Op {
    private List<AqlQueryNode> elements;

    protected OpN()         { elements = new ArrayList<>(); }
    protected OpN(List<AqlQueryNode> x)   { elements = x; }

    public void add(AqlQueryNode op) { elements.add(op); }
    public AqlQueryNode get(int idx) { return elements.get(idx); }

    //public abstract Op apply(Transform transform, List<Op> elts);
    public abstract OpN copy(List<AqlQueryNode> elts);

    public int size()                   { return elements.size(); }

    @Override
    public int hashCode()               { return elements.hashCode(); }

    public List<AqlQueryNode> getElements()           { return elements; }

    public Iterator<AqlQueryNode> iterator()          { return elements.iterator(); }
}
