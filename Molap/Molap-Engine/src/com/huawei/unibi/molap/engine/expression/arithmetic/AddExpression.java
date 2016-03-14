package com.huawei.unibi.molap.engine.expression.arithmetic;


import com.huawei.unibi.molap.engine.expression.DataType;
import com.huawei.unibi.molap.engine.expression.Expression;
import com.huawei.unibi.molap.engine.expression.ExpressionResult;
import com.huawei.unibi.molap.engine.expression.exception.FilterUnsupportedException;
import com.huawei.unibi.molap.engine.molapfilterinterface.ExpressionType;
import com.huawei.unibi.molap.engine.molapfilterinterface.RowIntf;

public class AddExpression extends BinaryArithmeticExpression
{
    private static final long serialVersionUID = 7999436055420911612L;

    public AddExpression(Expression left, Expression right)
    {
        super(left, right);
    }

    @Override
    public ExpressionResult evaluate(RowIntf value) throws FilterUnsupportedException
    {
        ExpressionResult addExprLeftRes = left.evaluate(value);
        ExpressionResult addExprRightRes = right.evaluate(value);
        ExpressionResult val1 = addExprLeftRes;
        ExpressionResult val2 = addExprRightRes;
        if(addExprLeftRes.isNull() || addExprRightRes.isNull()){
            addExprLeftRes.set(addExprLeftRes.getDataType(), null);
            return addExprLeftRes;
        }
        
        if(addExprLeftRes.getDataType() != addExprRightRes.getDataType())
        {
            if(addExprLeftRes.getDataType().getPresedenceOrder() < addExprRightRes.getDataType().getPresedenceOrder())
            {
                val2 = addExprLeftRes;
                val1 = addExprRightRes;
            }
        }
        switch(val1.getDataType())
        {
            case StringType:
            case DoubleType:
                addExprRightRes.set(DataType.DoubleType, val1.getDouble()+val2.getDouble());
                break;
          case IntegerType:
                addExprRightRes.set(DataType.IntegerType, val1.getInt()+val2.getInt());
            break;
        default:
            throw new FilterUnsupportedException("Incompatible datatype for applying Add Expression Filter " + val1.getDataType());
        }
        return addExprRightRes;
    }

    @Override
    public ExpressionType getFilterExpressionType()
    {
        return ExpressionType.ADD;
    }

    @Override
    public String getString()
    {
        return "Add(" + left.getString() + ',' + right.getString() + ',';
    }
}
