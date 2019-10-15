package com.aql.algebra;

import com.aql.algebra.operators.*;
import com.aql.algebra.resources.AssignedResource;
import com.aql.algebra.resources.GraphIterationResource;
import com.aql.algebra.resources.IterationResource;

public interface NodeVisitor
{
    // Op2
    void visit(OpFilter opFilter);
    void visit(OpNest opNest);

    // OpN
    //TODO consider using OpSequence to store a sequence of statements that are on the same level (sequence of stmts in the main scope, or sequence of stmts in a for loop)
    // ex. if we have a for loop that contains a let statement and another for loop, we can use an OpSequence variable inside OpFor class to store those 2
    void visit(OpSequence opSequence);

    // OpModifier
    void visit(OpSort opOrder);
    void visit(OpProject opProject);
    //void visit(OpDistinct opDistinct);
    void visit(OpLimit opLimit);
    void visit(OpCollect opGroup);

    //Resource
    void visit(IterationResource forloop);
    void visit(GraphIterationResource graph_forloop);
    void visit(AssignedResource assignment);
}
