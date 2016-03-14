package com.huawei.unibi.molap.engine.evaluators;

import java.util.BitSet;

import com.huawei.unibi.molap.engine.schema.metadata.FilterEvaluatorInfo;

public interface FilterEvaluator
{
    void resolve(FilterEvaluatorInfo info);
    
    BitSet applyFilter(BlockDataHolder blockDataHolder, FilterProcessorPlaceHolder placeHolder);
    
    FilterEvaluator getLeft();
    
    FilterEvaluator getRight();
    
    /**
     * This methods checks if filter has to be applied 
     * @param blockMaxValue
     * @param blockMinValue
     * @return
     */
    BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue);
}
