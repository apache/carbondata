package com.huawei.unibi.molap.engine.expression;


public abstract class BinaryExpression extends Expression
{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    /**
     * 
     */

    protected Expression left;
    protected Expression right;
    
    public BinaryExpression(Expression left,Expression right)
    {
        this.left=left;
        this.right=right;
        children.add(left);
        children.add(right);
    }

    public Expression getLeft()
    {
        return left;
    }

    public Expression getRight()
    {
        return right;
    }
    
    

    
}
