package com.sparql_to_aql.entities.algebra;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.core.Var;

import java.util.List;

public class OpDistinctProject extends OpProject {

    public OpDistinctProject(Op subOp, List<Var> vars)
    {
        super(subOp, vars);
    }

    @Override
    public String getName() { return "project_distinct"; }
}
