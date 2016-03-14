/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcz8AOhvEHjQfa55oxvUSJWRQCwLl+VwWEHaV7n
0eFj3QHR18ARx5f7xx6i1V9MPaumc+E/ZlW+EfpELyn+ucfJOJTyZaNYg3WylMro7oqdxtLX
fSCF4AMl+gDWUJ2AwxqfCDXw8wfuR+U1GaHmBYTDz1wwxP0pyzA7sEGOD/f9tw==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.filters.measurefilter;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;

/**
 * @author R00900208
 *
 */
public class AndMeasureGroupFilterImpl implements MeasureFilter,MeasureGroupFilter
{
    
    private MeasureFilter[][] measureFilters;
    
    private int[] msrFilterIndices;

    public AndMeasureGroupFilterImpl(MeasureFilter[][] measureFilters)
    {
        this.measureFilters = measureFilters;
        msrFilterIndices = MeasureFilterUtil.getMsrFilterIndexes(measureFilters);
    }

    @Override
    public boolean filter(MeasureAggregator[] msrValue)
    {
        for(int j = 0;j < msrFilterIndices.length;j++)
        {
            MeasureFilter[] measureFilter = measureFilters[msrFilterIndices[j]];
            for(int k = 0;k < measureFilter.length;k++)
            {
                if(!(measureFilter[k].filter(msrValue)))
                {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public boolean filter(double[] msrValue,int msrStartIndex)
    {
        for(int j = 0;j < msrFilterIndices.length;j++)
        {
            MeasureFilter[] measureFilter = measureFilters[msrFilterIndices[j]];
            for(int k = 0;k < measureFilter.length;k++)
            {
                if(!(measureFilter[k].filter(msrValue,msrStartIndex)))
                {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean isMsrFilterEnabled()
    {
        return msrFilterIndices.length>0;
    }

}
