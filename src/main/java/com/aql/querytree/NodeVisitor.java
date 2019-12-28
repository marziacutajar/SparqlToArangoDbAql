package com.aql.querytree;

import com.aql.querytree.operators.*;
import com.aql.querytree.resources.AssignedResource;
import com.aql.querytree.resources.GraphIterationResource;
import com.aql.querytree.resources.IterationResource;

public interface NodeVisitor
{
    // Op2
    void visit(OpFilter opFilter);
    void visit(OpNest opNest);

    // OpN
    //use OpSequence to store a sequence of statements that are on the same level (sequence of stmts in the main scope, or sequence of stmts in a for loop that aren't nested within each other)
    // ex. if we have a for loop that contains a let statement and another for loop, we can use an OpSequence variable inside OpFor class to store those 2
    void visit(OpSequence opSequence);

    // OpModifier
    void visit(OpSort opOrder);
    void visit(OpProject opProject);
    void visit(OpLimit opLimit);
    void visit(OpCollect opGroup);

    //Resource
    void visit(IterationResource forloop);
    void visit(GraphIterationResource graph_forloop);
    void visit(AssignedResource assignment);
}
