/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.huawei.unibi.molap.engine.expression.arithmetic;

import com.huawei.unibi.molap.engine.expression.DataType;
import com.huawei.unibi.molap.engine.expression.Expression;
import com.huawei.unibi.molap.engine.expression.ExpressionResult;
import com.huawei.unibi.molap.engine.expression.exception.FilterUnsupportedException;
import com.huawei.unibi.molap.engine.molapfilterinterface.ExpressionType;
import com.huawei.unibi.molap.engine.molapfilterinterface.RowIntf;

public class MultiplyExpression  extends BinaryArithmeticExpression 
{
    private static final long serialVersionUID = 1L;
    public MultiplyExpression(Expression left, Expression right)
    {
        super(left, right);
    }

    @Override
    public ExpressionResult evaluate(RowIntf value) throws FilterUnsupportedException
    {
        ExpressionResult multiplyExprLeftRes = left.evaluate(value);
        ExpressionResult multiplyExprRightRes = right.evaluate(value);
        ExpressionResult val1 = multiplyExprLeftRes;
        ExpressionResult val2 = multiplyExprRightRes;
        if(multiplyExprLeftRes.isNull() || multiplyExprRightRes.isNull())
        {
            multiplyExprLeftRes.set(multiplyExprLeftRes.getDataType(), null);
            return multiplyExprLeftRes;
        }
        
        if(multiplyExprLeftRes.getDataType() != multiplyExprRightRes.getDataType())
        {
            if(multiplyExprLeftRes.getDataType().getPresedenceOrder() < multiplyExprRightRes.getDataType().getPresedenceOrder())
            {
                val2 = multiplyExprLeftRes;
                val1 = multiplyExprRightRes;
            }
        }
        switch(val1.getDataType())
        {
        case StringType:
        case DoubleType:
            multiplyExprRightRes.set(DataType.DoubleType, val1.getDouble() * val2.getDouble());
            break;
        case IntegerType:
            multiplyExprRightRes.set(DataType.IntegerType, val1.getInt() * val2.getInt());
            break;
        default:
            throw new FilterUnsupportedException("Incompatible datatype for applying Add Expression Filter "
                    + multiplyExprLeftRes.getDataType());
        }
        return multiplyExprRightRes;
    }

    @Override
    public ExpressionType getFilterExpressionType()
    {
        return ExpressionType.MULTIPLY;
    }

    @Override
    public String getString()
    {
        return "Substract(" + left.getString() + ',' + right.getString() + ')';
    }
}
