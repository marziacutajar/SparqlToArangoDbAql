package com.aql.algebra;

import com.aql.algebra.expressions.*;
import com.aql.algebra.expressions.constants.Const_Bool;
import com.aql.algebra.expressions.constants.Const_Number;
import com.aql.algebra.expressions.constants.Const_String;
import com.aql.algebra.expressions.functions.ExprFunction0;
import com.aql.algebra.expressions.functions.ExprFunction1;
import com.aql.algebra.expressions.functions.ExprFunction2;
import com.aql.algebra.operators.*;
import com.sparql_to_aql.utils.AqlUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.sparql.expr.NodeValue;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AqlQuerySerializer implements OpVisitor, ExprVisitor {
    static final int BLOCK_INDENT = 5;

    private int CURRENT_INDENT;

    PrintWriter out;

    public AqlQuerySerializer(OutputStream _out)
    {
        out = new PrintWriter(_out);
        CURRENT_INDENT = 0;
    }

    private void indent(){
        out.print(StringUtils.repeat(" ", CURRENT_INDENT));
    }

    public void visit(OpFor opFor){
        indent();
        out.print("FOR " + opFor.getIterationVar() + " IN ");
        opFor.getDataArrayExpr().visit(this);
        out.println();
    }

    public void visit(OpFilter op){
        if(op.getSubOp() != null)
            op.getSubOp().visit(this);

        indent();
        out.print("FILTER ");
        ExprList filterExprs = op.getExprs();
        for(int i=0; i < filterExprs.size(); i++){
            filterExprs.get(i).visit(this);
            if(i < filterExprs.size()-1)
                out.print(AqlConstants.SYM_AND);
        }

        out.println();
    }

    public void visit(OpAssign opAssign){
        indent();
        out.print("LET " + opAssign.getVariableName() + "= ");

        if(opAssign.assignsExpr()){

        }else{
            out.print("(");
            opAssign.getOp().visit(this);
            out.print(")");
        }
        out.println();
    }

    //TODO consider: OpExtend doesn't really have any use... we can use OpNest only
    public void visit(OpExtend opExtend){
        opExtend.getSubOp().visit(this);
    }

    public void visit(OpNest opNest){
        opNest.getLeft().visit(this);
        CURRENT_INDENT += BLOCK_INDENT;
        opNest.getRight().visit(this);
    }

    public void visit(OpSort op){
        if(op.getSubOp() != null)
            op.getSubOp().visit(this);

        indent();
        out.print("SORT ");

        List<SortCondition> conditions = op.getConditions();
        for(SortCondition c: conditions){
            c.getExpression().visit(this);
            switch(c.getDirection()){
                case ASC:
                    out.print(" ASC");
                    break;
                case DESC:
                    out.print(" DESC");
                    break;
            }

            if(c != conditions.get(conditions.size() -1)){
                out.print(", ");
            }
        }
        out.println();
    }

    public void visit(OpProject op){
        if(op.getSubOp() != null)
            op.getSubOp().visit(this);

        indent();
        out.print("RETURN ");
        out.print("{");
        List<Expr> exprs = op.getExprs();

        for(int i=0; i < exprs.size(); i++){
            exprs.get(i).visit(this);
            if(i < exprs.size()-1 ){
                out.print(", ");
            }
        }

        out.println("}");
    }

    public void visit(OpLimit op){
        if(op.getSubOp() != null)
            op.getSubOp().visit(this);

        indent();
        out.println("LIMIT " + (op.getStart() > 0 ? op.getStart() + ", " : "") + op.getLength());
    }

    public void visit(OpCollect op){
        if(op.getSubOp() != null)
            op.getSubOp().visit(this);

        indent();
        out.print("COLLECT ");
        op.getGroupVars();
        op.getAggregators();
        out.println();
    }

    public void visit(ExprFunction1 expr){
        if(expr.getOpName() != null){
            out.print(expr.getOpName());
            expr.getArg().visit(this);
        }
    }

    public void visit(ExprFunction2 expr){
        if(expr.getOpName() != null){
            out.print("(");
            expr.getArg1().visit(this);
            out.print(expr.getOpName());
            expr.getArg2().visit(this);
            out.print(")");
        }
    }

    public void visit(Constant expr){
        if(expr instanceof Const_Bool){
            out.print(((Const_Bool)expr).getBoolean());
        } else if(expr instanceof Const_String){
            out.print(AqlUtils.quoteString(((Const_String)expr).getString()));
        } else if(expr instanceof Const_Number){
            out.print(((Const_Number)expr).getNumber());
        }
    }

    public void visit(ExprVar expr){
        out.print(expr.getVarName());
    }

    public void visit(Var var){
        out.print(var.getVarName());
    }

    public void visit(VarExprList exprs){
        Iterator<Map.Entry<Var, Expr>> it = exprs.getExprs().entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Var, Expr> pair = it.next();
            out.print(pair.getKey().getVarName() + ": ");
            pair.getValue().visit(this);
            if(it.hasNext()){
                out.print(", ");
            }
        }
    }

    public void visit(OpSequence opSequence){

    }

    public void finishVisit()
    {
        out.flush();
        CURRENT_INDENT = 0;
    }
}
