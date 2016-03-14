package com.huawei.unibi.molap.engine.columnar.filter;

import java.util.BitSet;

import org.uncommons.maths.Maths;

import com.huawei.unibi.molap.engine.filters.metadata.InMemFilterModel;

public abstract class AbstractColumnarFilterProcessor implements ColumnarFilterProcessor 
{
    
    /**
     * filter model
     */
    protected InMemFilterModel filterModel;
    
    public AbstractColumnarFilterProcessor(InMemFilterModel filterModel)
    {
        this.filterModel=filterModel;
    }
    protected boolean useBitSet(BitSet set, byte[][] filter, int numerOfRows)
    {
        return calculateBitSetExecutionCost(set, filter.length) < calculateBinarySearchCost(filter, numerOfRows);
    }

    private double calculateBitSetExecutionCost(BitSet set, int numberOfFilters)
    {
        double size = set.cardinality();
        double logValue=Maths.log(2, numberOfFilters);
        return (size * (logValue>0?logValue:1));
    }
    
    private double calculateBinarySearchCost(byte[][] filter,int numerOfRows)
    {
        double logValue=Maths.log(2, numerOfRows);
        return (filter.length* 2 * (logValue>0?logValue:1));
    }
    
}
