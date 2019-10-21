package com.aql.algebra;

import com.aql.algebra.expressions.*;
import com.aql.algebra.expressions.functions.*;
import com.aql.algebra.operators.*;
import com.aql.algebra.resources.AssignedResource;
import com.aql.algebra.resources.GraphIterationResource;
import com.aql.algebra.resources.IterationResource;
import org.apache.jena.atlas.io.IndentedWriter;

import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AqlAlgebraTreeWriter implements NodeVisitor, ExprVisitor {
    static final int BLOCK_INDENT = 2;

    IndentedWriter out;

    public AqlAlgebraTreeWriter(OutputStream _out)
    {
        out = new IndentedWriter(_out);
        out.setUnitIndent(BLOCK_INDENT);
    }

    private void visitOpN(OpN op) {
        start(op, true);
        for (Iterator<Op> iter = op.iterator(); iter.hasNext();)
        {
            Op sub = iter.next();
            out.println();
            sub.visit(this);
        }
        finish(op) ;
    }

    private void visitOp2(Op2 op, ExprList exprs) {
        start(op, true);
        op.getLeft().visit(this);
        op.getRight().visit(this);

        /*if (exprs != null) {
            out.ensureStartOfLine() ;
            WriterExpr.output(out, exprs, sContext) ;
        }*/
        finish(op) ;
    }

    private void visitOp1(Op1 op) {
        start(op, true) ;
        op.getChild().visit(this);
        finish(op) ;
    }

    public void visit(IterationResource forloop){
        start(forloop, false);
        start();
        boolean useBrackets = false;
        out.print(forloop.getIterationVar().getVarName());
        out.print(" ");
        Expr dataArrayExpr = forloop.getDataArrayExpr();
        if(!(dataArrayExpr instanceof Var)) {
            useBrackets = true;
            out.print("(");
        }

        forloop.getDataArrayExpr().visit(this);
        if(useBrackets)
            out.print(")");

        finish();
        out.println();
    }

    public void visit(GraphIterationResource graphForloop){
        out.print("FOR " + graphForloop.getVertexVar());

        if(graphForloop.getEdgeVar() != null){
            out.print(", " + graphForloop.getEdgeVar());

            if(graphForloop.getPathVar() != null)
                out.print(", " + graphForloop.getPathVar());
        }

        out.print(" IN ");

        if(graphForloop.getMin() != null){
            out.print(graphForloop.getMin().toString());
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
        start(op, false);
        ExprList filterExprs = op.getExprs();
        for(int i=0; i < filterExprs.size(); i++){
            filterExprs.get(i).visit(this);
            if(i < filterExprs.size()-1)
                out.print(AqlConstants.SYM_AND);
        }
        out.println();
        op.getChild().visit(this);

        finish(op);
    }

    public void visit(AssignedResource opAssign){
        start(opAssign, false);
        start();
        out.print(opAssign.getVariableName());
        out.print(", ");
        if(opAssign.assignsExpr()){
            opAssign.getExpr().visit(this);
        }else{
            out.print("(");
            opAssign.getOp().visit(this);
            out.print(")");
        }
        finish();
        out.println();
    }

    public void visit(OpNest opNest){
        visitOp2(opNest, null);
    }

    public void visit(OpSort op){
        start(op, true);

        // Write conditions
        start();

        boolean first = true ;
        for (SortCondition sc : op.getConditions()) {
            if (!first)
                out.print(" ");
            first = false ;
            formatSortCondition(sc);
        }
        finish() ;
        out.println();
        op.getChild().visit(this); ;
        finish(op);
    }

    private void formatSortCondition(SortCondition sc) {
        String tag = null;

        if(sc.getDirection() == SortCondition.Direction.ASC)
        {
            tag = "ASC";
            start(tag, false);
        }
        else if(sc.getDirection() == SortCondition.Direction.DESC) {
            tag = "DESC";
            start(tag, false);
        }

        //TODO
        //WriterExpr.output(out, sc.getExpression(), sContext) ;

        if (tag != null)
            finish();
    }

    public void visit(OpProject op){
        start(op, false) ;
        //writeVarList(op.getExprs());
        out.println();
        op.getChild().visit(this);
        finish(op);
    }

    public void visit(OpLimit op){
        start(op, false) ;
        //writeIntOrDefault(op.getStart());
        out.print(" ") ;
        //writeIntOrDefault(op.getLength());
        out.println() ;
        op.getChild().visit(this);
        finish(op) ;
    }

    public void visit(OpCollect op){
        op.getChild().visit(this);

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
            out.print(expr.getOpName());
            out.print(" ");
            expr.getArg1().visit(this);
            out.print(" ");
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

    }

    public void finishVisit()
    {
        out.flush();
        out.setAbsoluteIndent(0);
    }

    private void start(AqlQueryNode op, boolean newline) {
        start(op.getName(), newline);
    }

    private void start(String tag, boolean newline) {
        out.print("(");
        out.print(tag);

        if(newline)
            out.println();

        out.incIndent();
    }

    private void start() {
        out.print("(");
    }

    private void finish(Op op) {
        out.print(")");
        out.decIndent();
    }

    private void finish() {
        out.print(")");
    }
}
