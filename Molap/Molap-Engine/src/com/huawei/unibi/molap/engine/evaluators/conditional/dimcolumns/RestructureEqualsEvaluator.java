package com.huawei.unibi.molap.engine.evaluators.conditional.dimcolumns;

import java.util.BitSet;

import com.huawei.unibi.molap.engine.evaluators.AbastractRSConditionalEvalutor;
import com.huawei.unibi.molap.engine.evaluators.BlockDataHolder;
import com.huawei.unibi.molap.engine.evaluators.FilterProcessorPlaceHolder;
import com.huawei.unibi.molap.engine.expression.Expression;

public class RestructureEqualsEvaluator extends AbastractRSConditionalEvalutor
{

    public RestructureEqualsEvaluator(Expression exp, String defaultValue, int surrogate,boolean isExpressionResolve)
    {
        super(exp, defaultValue,surrogate,isExpressionResolve,true);
    }

    @Override
    public BitSet applyFilter(BlockDataHolder blockDataHolder, FilterProcessorPlaceHolder placeHolder)
    {
        BitSet bitSet = new BitSet(blockDataHolder.getLeafDataBlock().getnKeys());
        byte[][] filterValues = dimColEvaluatorInfoList.get(0).getFilterValues();
        if(null!=filterValues && filterValues.length>0)
        {
            bitSet.set(0, blockDataHolder.getLeafDataBlock().getnKeys());
        }
        return bitSet;
    }
    
    @Override
    public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue)
    {
        BitSet bitSet = new BitSet(1);
        bitSet.set(0);
        return bitSet;
    }
}
