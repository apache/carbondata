package com.huawei.unibi.molap.engine.expression.conditional;

import java.util.ArrayList;
import java.util.List;

import com.huawei.unibi.molap.engine.expression.Expression;
import com.huawei.unibi.molap.engine.expression.ExpressionResult;
import com.huawei.unibi.molap.engine.expression.exception.FilterUnsupportedException;
import com.huawei.unibi.molap.engine.molapfilterinterface.ExpressionType;
import com.huawei.unibi.molap.engine.molapfilterinterface.RowIntf;

public class ListExpression extends Expression
{
    private static final long serialVersionUID = 1L;
    public ListExpression(List<Expression> children)
    {
        this.children = children;
    }

    @Override
    public ExpressionResult evaluate(RowIntf value) throws FilterUnsupportedException
    {
         List<ExpressionResult> listOfExprRes=new ArrayList<ExpressionResult>(10);
         
         for(Expression expr:children)
         {
             listOfExprRes.add(expr.evaluate(value));
         }
        return new ExpressionResult(listOfExprRes);
    }

    @Override
    public ExpressionType getFilterExpressionType()
    {
        // TODO Auto-generated method stub
        return ExpressionType.LIST;
    }

    @Override
    public String getString()
    {
        // TODO Auto-generated method stub
        return null;
    }

}
