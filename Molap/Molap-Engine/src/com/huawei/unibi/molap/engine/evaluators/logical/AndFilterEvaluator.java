package com.huawei.unibi.molap.engine.evaluators.logical;

import java.util.BitSet;

import com.huawei.unibi.molap.engine.evaluators.BlockDataHolder;
import com.huawei.unibi.molap.engine.evaluators.FilterEvaluator;
import com.huawei.unibi.molap.engine.evaluators.FilterProcessorPlaceHolder;

public class AndFilterEvaluator extends AbstractLogicalFilterEvaluator
{

    public AndFilterEvaluator(FilterEvaluator leftEvalutor, FilterEvaluator rightEvalutor)
    {
        super(leftEvalutor, rightEvalutor);
    }

    @Override
    public BitSet applyFilter(BlockDataHolder blockDataHolder, FilterProcessorPlaceHolder placeHolder)
    {
        BitSet leftFilters = leftEvalutor.applyFilter(blockDataHolder,placeHolder);
        if(leftFilters.isEmpty())
        {
            return leftFilters;
        }
        BitSet rightFilter = rightEvalutor.applyFilter(blockDataHolder,placeHolder);
        if(rightFilter.isEmpty())
        {
            return rightFilter;
        }
        leftFilters.and(rightFilter);
        return leftFilters;
    }

    @Override
    public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue)
    {
        BitSet leftFilters = leftEvalutor.isScanRequired(blockMaxValue, blockMinValue);
        if(leftFilters.isEmpty())
        {
            return leftFilters;
        }
        BitSet rightFilter = rightEvalutor.isScanRequired(blockMaxValue, blockMinValue);
        if(rightFilter.isEmpty())
        {
            return rightFilter;
        }
        leftFilters.and(rightFilter);
        return leftFilters;
    }

}
