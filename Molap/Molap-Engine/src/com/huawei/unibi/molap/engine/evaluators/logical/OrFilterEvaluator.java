package com.huawei.unibi.molap.engine.evaluators.logical;

import java.util.BitSet;

import com.huawei.unibi.molap.engine.evaluators.BlockDataHolder;
import com.huawei.unibi.molap.engine.evaluators.FilterEvaluator;
import com.huawei.unibi.molap.engine.evaluators.FilterProcessorPlaceHolder;

public class OrFilterEvaluator extends AbstractLogicalFilterEvaluator
{
    public OrFilterEvaluator(FilterEvaluator leftEvalutor, FilterEvaluator rightEvalutor)
    {
        super(leftEvalutor, rightEvalutor);
    }

    @Override
    public BitSet applyFilter(BlockDataHolder blockDataHolder, FilterProcessorPlaceHolder placeHolder)
    {
        BitSet leftFilters = leftEvalutor.applyFilter(blockDataHolder,placeHolder);
        BitSet rightFilters = rightEvalutor.applyFilter(blockDataHolder,placeHolder);
        leftFilters.or(rightFilters);
        return leftFilters;
    }

    @Override
    public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue)
    {
        BitSet leftFilters = leftEvalutor.isScanRequired(blockMaxValue, blockMinValue);
        BitSet rightFilters = rightEvalutor.isScanRequired(blockMaxValue, blockMinValue);
        leftFilters.or(rightFilters);
        return leftFilters;
    }

}
