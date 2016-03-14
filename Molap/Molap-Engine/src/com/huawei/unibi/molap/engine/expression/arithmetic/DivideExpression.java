package com.huawei.unibi.molap.engine.expression.arithmetic;

import com.huawei.unibi.molap.engine.expression.DataType;
import com.huawei.unibi.molap.engine.expression.Expression;
import com.huawei.unibi.molap.engine.expression.ExpressionResult;
import com.huawei.unibi.molap.engine.expression.exception.FilterUnsupportedException;
import com.huawei.unibi.molap.engine.molapfilterinterface.ExpressionType;
import com.huawei.unibi.molap.engine.molapfilterinterface.RowIntf;

public class DivideExpression extends BinaryArithmeticExpression 
{
    private static final long serialVersionUID = -7269266926782365612L;

    public DivideExpression(Expression left, Expression right)
    {
        super(left, right);
    }
    
    @Override
    public ExpressionResult evaluate(RowIntf value) throws FilterUnsupportedException
    {
        ExpressionResult divideExprLeftRes = left.evaluate(value);
        ExpressionResult divideExprRightRes = right.evaluate(value);
        ExpressionResult val1 = divideExprLeftRes;
        ExpressionResult val2 = divideExprRightRes;
        if(divideExprLeftRes.isNull() || divideExprRightRes.isNull()){
            divideExprLeftRes.set(divideExprLeftRes.getDataType(), null);
            return divideExprLeftRes;
        }
        if(divideExprLeftRes.getDataType() != divideExprRightRes.getDataType())
        {
            if(divideExprLeftRes.getDataType().getPresedenceOrder() < divideExprRightRes.getDataType().getPresedenceOrder())
            {
                val2 = divideExprLeftRes;
                val1 = divideExprRightRes;
            }
        }
        switch(val1.getDataType())
        {
            case StringType:
            case DoubleType:
                divideExprRightRes.set(DataType.DoubleType, val1.getDouble()/val2.getDouble());
                break;
          case IntegerType:
            divideExprRightRes.set(DataType.IntegerType, val1.getInt()/val2.getInt());
            break;
        default:
            throw new FilterUnsupportedException("Incompatible datatype for applying Add Expression Filter " + divideExprLeftRes.getDataType());
        }
        return divideExprRightRes;
    }

    @Override
    public ExpressionType getFilterExpressionType()
    {
        return ExpressionType.DIVIDE;
    }

    @Override
    public String getString()
    {
        return "Divide(" + left.getString() + ',' + right.getString() + ')';
    }
}
