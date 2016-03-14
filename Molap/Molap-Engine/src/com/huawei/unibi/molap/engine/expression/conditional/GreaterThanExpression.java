package com.huawei.unibi.molap.engine.expression.conditional;

import com.huawei.unibi.molap.engine.expression.DataType;
import com.huawei.unibi.molap.engine.expression.Expression;
import com.huawei.unibi.molap.engine.expression.ExpressionResult;
import com.huawei.unibi.molap.engine.expression.exception.FilterUnsupportedException;
import com.huawei.unibi.molap.engine.molapfilterinterface.ExpressionType;
import com.huawei.unibi.molap.engine.molapfilterinterface.RowIntf;

public class GreaterThanExpression extends BinaryConditionalExpression
{
    private static final long serialVersionUID = -5319109756575539219L;
    public GreaterThanExpression(Expression left, Expression right)
    {
        super(left, right);
        // TODO Auto-generated constructor stub
    }

    @Override
    public ExpressionResult evaluate(RowIntf value) throws FilterUnsupportedException
    {
        ExpressionResult exprLeftRes = left.evaluate(value); 
        ExpressionResult exprRightRes = right.evaluate(value);  
        ExpressionResult val1 = exprLeftRes;
        if(exprLeftRes.isNull() || exprRightRes.isNull()){
            exprLeftRes.set(DataType.BooleanType, false);
            return exprLeftRes;
        }
        if(exprLeftRes.getDataType() != exprRightRes.getDataType())
        {
            if(exprLeftRes.getDataType().getPresedenceOrder() < exprRightRes.getDataType().getPresedenceOrder())
            {
                val1 = exprRightRes;
            }
            
        }
        boolean result = false;
        switch(val1.getDataType())
        {
        case StringType:
            result = exprLeftRes.getString().compareTo(exprRightRes.getString())>0;
            break;
        case DoubleType:
            result = exprLeftRes.getDouble()>(exprRightRes.getDouble());
            break;
        case IntegerType:
            result = exprLeftRes.getInt()>(exprRightRes.getInt());
            break;
        case TimestampType:
            result = exprLeftRes.getTime()>(exprRightRes.getTime());
            break;
        default:
            break;
        }
        val1.set(DataType.BooleanType, result);
        return val1;
    }

    @Override
    public ExpressionType getFilterExpressionType()
    {
        return ExpressionType.GREATERTHAN;
    }

    @Override
    public String getString()
    {
        return "GreaterThan(" + left.getString() + ',' + right.getString() + ')';
    }

}
