package com.aql.algebra.operators;

import com.aql.algebra.AqlQueryNode;
import com.aql.algebra.NodeVisitor;
import com.aql.algebra.resources.AssignedResource;
import org.apache.jena.sparql.algebra.op.OpAssign;

import java.util.ArrayList;
import java.util.List;

//TODO consider removing this if we're going to use OpNest
//difference between this Op and OpAssign is that OpExtend is used for nested LETs and OpAssign
//is used to assign a whole subOp or expression into a variable
public class OpExtend extends Op1{

    //keep a list of assignments instead of one, to reduce nested operations
    private List<AssignedResource> assignments;

    public OpExtend(AqlQueryNode sub, AssignedResource assignment) {
        super(sub);
        this.assignments = new ArrayList<>();
        this.assignments.add(assignment);
    }

    public OpExtend(AqlQueryNode sub, List<AssignedResource> assignments) {
        super(sub);
        this.assignments = assignments;
    }

    @Override
    public String getName() { return "extend"; }

    public List<AssignedResource> getAssignments(){
        return assignments;
    }

    @Override
    public void visit(NodeVisitor visitor) { visitor.visit(this); }

    @Override
    public Op1 copy(AqlQueryNode sub){
        return new OpExtend(sub, assignments);
    }

}
