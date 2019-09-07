package com.aql.algebra;

import com.aql.algebra.operators.*;

public interface OpVisitor
{
    // Op0
    public void visit(OpFor opFor);

    //Op1
    public void visit(OpFilter opFilter);
    public void visit(OpAssign opAssign);
    public void visit(OpExtend opExtend);

    // Op2
    //public void visit(OpUnion opUnion);
    //public void visit(OpMinus opMinus);
    public void visit(OpNest opNest);

    // OpN
    //TODO consider using OpSequence to store a sequence of statements that are on the same level (sequence of stmts in the main scope, or sequence of stmts in a for loop)
    // ex. if we have a for loop that contains a let statement and another for loop, we can use an OpSequence variable inside OpFor class to store those 2
    public void visit(OpSequence opSequence);
    //public void visit(OpDisjunction opDisjunction);

    // OpModifier
    //public void visit(OpList opList);
    public void visit(OpSort opOrder);
    public void visit(OpProject opProject);
    //public void visit(OpDistinct opDistinct);
    public void visit(OpLimit opLimit);
    public void visit(OpCollect opGroup);
}

