package com.sparql_to_aql.entities.algebra.aql;

import com.sparql_to_aql.entities.algebra.aql.operators.*;

public interface OpVisitor
{
    // Op0
    public void visit(OpFor opFor);

    //Op1
    public void visit(OpFilter opFilter);
    //public void visit(AqlOpGraph opGraph);
    //public void visit(OpDatasetNames dsNames);
    //public void visit(OpLabel opLabel);
    public void visit(OpAssign opAssign);
    //public void visit(AqlOpExtend opExtend);

    // Op2
    public void visit(OpUnion opUnion);
    public void visit(OpMinus opMinus);

    // OpN
    //TODO consider using OpSequence to store a sequence of subqueries that are on the same level
    // ex. if we have a for loop that contains a let statement and another for loop, we can use an OpSequence variable
    // in the FOR operator class to store those 2
    //public void visit(AqlOpSequence opSequence);
    //public void visit(OpDisjunction opDisjunction);

    // OpModifier
    //public void visit(OpList opList);
    public void visit(OpSort opOrder);
    public void visit(OpProject opProject);
    //public void visit(OpDistinct opDistinct);
    public void visit(OpLimit opLimit);
    public void visit(OpCollect opGroup);
}

