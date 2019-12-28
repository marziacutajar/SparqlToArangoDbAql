package com.aql.querytree;

import com.aql.querytree.expressions.Expr;
import com.aql.querytree.expressions.ExprVar;
import com.aql.querytree.expressions.Var;

public class SortCondition {
    public Expr expression;
    public Direction direction;

    public enum Direction {
        ASC,
        DESC,
        DEFAULT
    }

    public SortCondition(Var var, Direction dir){
        this(new ExprVar(var), dir);
    }

    public SortCondition(Expr expr, Direction dir)
    {
        expression = expr;
        direction = dir;
    }

    /** @return Returns the direction. */
    public Direction getDirection()
    {
        return direction;
    }

    /** @return Returns the expression. */
    public Expr getExpression()
    {
        return expression;
    }
}
