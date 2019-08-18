package com.sparql_to_aql.entities.algebra.aql.operators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class OpN implements Op {
    private List<Op> elements = new ArrayList<>() ;

    protected OpN()         { elements = new ArrayList<>() ; }
    protected OpN(List<Op> x)   { elements = x ; }

    public void add(Op op) { elements.add(op) ; }
    public Op get(int idx) { return elements.get(idx) ; }

    //public abstract Op apply(Transform transform, List<Op> elts) ;
    public abstract OpN copy(List<Op> elts) ;

    public int size()                   { return elements.size() ; }


    @Override
    public int hashCode()               { return elements.hashCode() ; }

    public List<Op> getElements()           { return elements ; }

    public Iterator<Op> iterator()          { return elements.iterator() ; }
}
