package com.huawei.unibi.molap.engine.expression;

import com.huawei.unibi.molap.engine.molapfilterinterface.ExpressionType;
import com.huawei.unibi.molap.engine.molapfilterinterface.RowIntf;

public class LiteralExpression extends LeafExpression
{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private Object value;
    private DataType dataType;
    
    public LiteralExpression(Object value,DataType dataType)
    {
        this.value=value;
        this.dataType=dataType;
        
        
        /*switch(dataType)
        {
        case StringType:
            this.value = value;
            break;
        case IntegerType:
            this.value = Integer.parseInt(value);
            break;
        case DoubleType:
            this.value = Double.parseDouble(value);
            break;
        default:
            break;
        }*/
        
    }
    @Override
    public ExpressionResult evaluate(RowIntf value)
    {
        ExpressionResult expressionResult = new ExpressionResult(dataType, this.value);
        return expressionResult;
    }
    @Override
    public ExpressionType getFilterExpressionType()
    {
        // TODO Auto-generated method stub
        return ExpressionType.LITERAL;
    }
    @Override
    public String getString()
    {
        // TODO Auto-generated method stub
        return "LiteralExpression("+value+')';
    }


}
