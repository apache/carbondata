/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcz8AOhvEHjQfa55oxvUSJWRQCwLl+VwWEHaV7n
0eFj3UduExwP8QknaZ+KHlfTaNkvNK91OuUYcg1Ua7eg9zEyrOb0kvHuyaHNttxVen1vxYeN
tNeaX+JNCXjkuBb0MVX5HSyio8HB0nY/OOlHFI1x3G2MWYL/oHvyqNcVo7mB/w==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
 */
package com.huawei.unibi.molap.engine.filters.measurefilter;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;

/**
 * It is interface class for Measure filter
 * @author R00900208
 *
 */
public interface MeasureFilter
{
    /**
     * Filter the measure as per the passed value satisfies the implemented class condition.
     * 
     * @param msrValue
     * @return
     */
    boolean filter(MeasureAggregator[] msrValue);
    
    /**
     * Filter the measure as per the passed value satisfies the implemented class condition.
     * 
     * @param msrValue
     * @return
     */
    boolean filter(double[] msrValue,int msrStartIndex);
}
