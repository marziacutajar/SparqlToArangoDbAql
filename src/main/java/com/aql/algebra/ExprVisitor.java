package com.aql.algebra;

import com.aql.algebra.expressions.Constant;
import com.aql.algebra.expressions.ExprAggregator;
import com.aql.algebra.expressions.ExprVar;
import com.aql.algebra.expressions.Var;
import com.aql.algebra.expressions.functions.*;

public interface ExprVisitor {
    //void visit(ExprFunction0 func) ;
    void visit(ExprFunction1 func) ;
    void visit(ExprFunction2 func) ;
    //void visit(ExprFunction3 func) ;
    //void visit(ExprFunctionN func) ;
    void visit(Constant c) ;
    void visit(ExprVar ev) ;
    //void visit(ExprAggregator eAgg) ;
    void visit(Var v) ;
}
