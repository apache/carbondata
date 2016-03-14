package com.huawei.unibi.molap.engine.expression;

public abstract class UnaryExpression extends Expression
{

    private static final long serialVersionUID = 1L;
    protected Expression child;
    
    public UnaryExpression(Expression child)
    {
        this.child=child;
        children.add(child);
        
    }
    
}
