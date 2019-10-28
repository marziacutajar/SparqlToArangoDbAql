package com.aql.algebra;

import com.aql.algebra.expressions.*;
import com.aql.algebra.expressions.functions.*;
import com.aql.algebra.operators.*;
import com.aql.algebra.resources.AssignedResource;
import com.aql.algebra.resources.GraphIterationResource;
import com.aql.algebra.resources.IterationResource;
import org.apache.commons.lang3.StringUtils;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AqlQuerySerializer implements NodeVisitor, ExprVisitor {
    static final int BLOCK_INDENT = 5;

    private int CURRENT_INDENT;

    PrintWriter out;

    public AqlQuerySerializer(OutputStream _out)
    {
        out = new PrintWriter(_out);
        CURRENT_INDENT = 0;
    }

    public AqlQuerySerializer(StringWriter _out)
    {
        out = new PrintWriter(_out);
        CURRENT_INDENT = 0;
    }

    private void indent(){
        out.print(StringUtils.repeat(" ", CURRENT_INDENT));
    }

    public void visit(IterationResource forloop){
        boolean useBrackets = false;
        indent();
        out.print("FOR " + forloop.getIterationVar().getVarName() + " IN ");
        Expr dataArrayExpr = forloop.getDataArrayExpr();
        if(!(dataArrayExpr instanceof Var)) {
            useBrackets = true;
            out.print("(");
        }

        forloop.getDataArrayExpr().visit(this);
        if(useBrackets)
            out.print(")");

        out.println();
    }

    public void visit(GraphIterationResource graphForloop){
        indent();
        out.print("FOR " + graphForloop.getVertexVar());

        if(graphForloop.getEdgeVar() != null){
            out.print(", " + graphForloop.getEdgeVar());

            if(graphForloop.getPathVar() != null)
                out.print(", " + graphForloop.getPathVar());
        }

        out.print(" IN ");

        if(graphForloop.getMin() != null){
            out.print(graphForloop.getMin());
            if(graphForloop.getMax() != null)
                out.print(".." + graphForloop.getMax());
        }

        out.print(" " + graphForloop.getDirectionAsString() + " " + graphForloop.getStartVertex() + " ");

        if(graphForloop.getGraph()!= null){
            out.print(" GRAPH " + graphForloop.getGraph());
        }
        else{
            out.print(String.join(", ", graphForloop.getEdgeCollections()));
        }

        out.println();
    }

    public void visit(OpFilter op){
        op.getChild().visit(this);

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

    public void visit(AssignedResource opAssign){
        indent();
        out.print("LET " + opAssign.getVariableName() + " = ");

        if(opAssign.assignsExpr()){
            opAssign.getExpr().visit(this);
        }else{
            out.print("(");
            opAssign.getOp().visit(this);
            out.print(")");
        }
        out.println();
    }

    public void visit(OpNest opNest){
        opNest.getLeft().visit(this);
        CURRENT_INDENT += BLOCK_INDENT;
        opNest.getRight().visit(this);
    }

    public void visit(OpSort op){
        op.getChild().visit(this);

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
        op.getChild().visit(this);

        List<Expr> exprs = op.getExprs();
        boolean useBrackets = false;
        if(exprs.size() == 1 && exprs.get(0) instanceof VarExprList){
            useBrackets = true;
        }
        indent();
        out.print("RETURN ");

        if(op.isDistinct())
            out.print("DISTINCT ");

        if(useBrackets)
            out.print("{");

        for(int i=0; i < exprs.size(); i++){
            exprs.get(i).visit(this);
            if(i < exprs.size()-1 ){
                out.print(", ");
            }
        }

        if(useBrackets)
            out.println("}");
    }

    public void visit(OpLimit op){
        op.getChild().visit(this);

        indent();
        out.println("LIMIT " + (op.getStart() > 0 ? op.getStart() + ", " : "") + op.getLength());
    }

    public void visit(OpCollect op){
        op.getChild().visit(this);

        indent();
        out.print("COLLECT ");
        op.getVarExprs().visit(this);

        if(op.isWithCount())
            out.print("WITH COUNT INTO " + op.getCountVar().getVarName());
        out.println();
    }

    public void visit(ExprFunction1 expr){
        if(expr.getOpName() != null){
            out.print(expr.getOpName());
            expr.getArg().visit(this);
        }
        else{
            out.print(expr.getFunctionName() + "(");
            expr.getArg().visit(this);
            out.print(")");
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
        else if(expr instanceof Expr_In){
            expr.getArg1().visit(this);
            out.print(" IN ");
            expr.getArg2().visit(this);
        }
    }

    public void visit(ExprFunction3 expr){
        if(expr instanceof Expr_Conditional){
            expr.getArg1().visit(this);
            out.print(" ? ");
            expr.getArg2().visit(this);
            out.print(" : ");
            expr.getArg3().visit(this);
        }
    }

    public void visit(ExprFunctionN expr){
        out.print(expr.getFunctionName() + "(");
        List<Expr> functionArgs = expr.getArgs();
        for(int i=0; i < functionArgs.size(); i++){
            functionArgs.get(i).visit(this);
            if(i < functionArgs.size()-1)
                out.print(",");
        }
        out.print(")");
    }

    public void visit(Constant expr){
        out.print(expr.toString());
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
        for (Iterator<AqlQueryNode> iter = opSequence.iterator(); iter.hasNext();)
        {
            AqlQueryNode sub = iter.next();
            sub.visit(this);
            CURRENT_INDENT = 0;
        }
    }

    public void finishVisit()
    {
        out.flush();
        CURRENT_INDENT = 0;
    }
}
