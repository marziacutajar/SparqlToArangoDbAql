package com.aql.algebra;

import com.aql.algebra.expressions.ExprVar;
import com.aql.algebra.expressions.functions.ExprFunction0;
import com.aql.algebra.expressions.functions.ExprFunction1;
import com.aql.algebra.expressions.functions.ExprFunction2;
import com.aql.algebra.operators.*;
import org.apache.jena.sparql.expr.NodeValue;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.List;

public class AqlQuerySerializer implements OpVisitor, ExprVisitor {
    static final int BLOCK_INDENT = 2 ;

    PrintWriter out;

    public AqlQuerySerializer(OutputStream _out)
    {
        out = new PrintWriter(_out);
    }

    public void visit(OpFor opFor){
        //TODO visit expr here and print out its serialization
        out.print("FOR " + opFor.getIterationVar() + " IN ");
    }

    public void visit(OpFilter opFilter){
        out.print("FILTER ");
        opFilter.getExprs();
        //TODO visit exprs here and print out its serialization

    }

    public void visit(OpAssign opAssign){
        out.print("LET " + opAssign.getVariableName() + "= ");

        if(opAssign.assignsExpr()){

        }else{
            out.print("(");
            opAssign.getOp().visit(this);
            out.print(")");
        }
    }

    //TODO consider: OpExtend doesn't really have any use... we can use OpNest only
    public void visit(OpExtend opExtend){

    }

    public void visit(OpNest opNest){

    }

    public void visit(OpSort opSort){
        out.print("SORT ");

        List<SortCondition> conditions = opSort.getConditions();
        for(SortCondition c: conditions){
            //TODO process sort expr and print
            switch(c.getDirection()){
                case ASC:
                    out.print("ASC");
                    break;
                case DESC:
                    out.print("DESC");
                    break;
            }

            if(c != conditions.get(conditions.size() -1)){
                out.print(",");
            }
        }
    }

    public void visit(OpProject opProject){
        out.print("RETURN ");
        opProject.getExprs();
        //TODO visit exprs here and print out its serialization

    }

    public void visit(OpLimit opLimit){
        out.print("LIMIT " + opLimit.getStart() + ", " + opLimit.getLength());
    }

    public void visit(OpCollect opCollect){
        out.print("COLLECT ");
        opCollect.getGroupVars();
        opCollect.getAggregators();
    }

    public void visit(ExprFunction1 expr){

    }

    public void visit(ExprFunction2 expr){

    }

    public void visit(NodeValue expr){

    }

    public void visit(ExprVar expr){

    }

    public void visit(OpSequence opSequence){

    }

    public void finishVisit()
    {
        out.flush();
    }
}
