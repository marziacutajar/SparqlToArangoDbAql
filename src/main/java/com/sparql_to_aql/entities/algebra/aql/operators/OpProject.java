package com.sparql_to_aql.entities.algebra.aql;

import java.util.ArrayList;
import java.util.List;

public class OpProject extends OpModifier {
    private List<Var> vars = new ArrayList<>();

    public OpProject(Op subOp, List<Var> vars)
    {
        super(subOp);
        this.vars = vars;
    }

    public List<Var> getVars() { return vars; }

    @Override
    public String getName() { return AqlConstants.keywordReturn; }
    @Override
    public void visit(OpVisitor opVisitor)  { opVisitor.visit(this); }
    @Override
    public Op1 copy(Op subOp)                { return new OpProject(subOp, vars) ; }

    /*@Override
    public Op apply(Transform transform, Op subOp)
    { return transform.transform(this, subOp) ; }*/

    @Override
    public int hashCode()
    {
        return vars.hashCode() ^ getSubOp().hashCode() ;
    }

    /*@Override
    public boolean equalTo(Op other, NodeIsomorphismMap labelMap)
    {
        if ( ! (other instanceof OpProject) ) return false ;
        OpProject opProject = (OpProject)other ;
        if ( ! Objects.equals(vars, opProject.vars ) )
            return false ;
        return getSubOp().equalTo(opProject.getSubOp(), labelMap) ;
    }*/
}
