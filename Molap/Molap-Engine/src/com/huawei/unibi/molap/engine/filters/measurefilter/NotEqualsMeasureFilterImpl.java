/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcz8AOhvEHjQfa55oxvUSJWRQCwLl+VwWEHaV7n
0eFj3T17ydhXFZwpCkzWBtfBNbnMvhXdpY2mWR8VLJdY1KMiF2GmktL+G7ueVLJkYYTOm9C0
KjViZGVmFG6oaTxSi56ZbWmBiRH1XMhrJXKGe8xj3wRKt7aPtmQLsyAqMnQSPQ==*/
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
import com.huawei.unibi.molap.engine.executer.calcexp.MolapCalcFunction;

/**
 * NotEqualsMeasureFilter
 * @author R00900208
 *
 */
public class NotEqualsMeasureFilterImpl implements MeasureFilter
{

    private double filterValue;
    
    private int index;
    
    private MolapCalcFunction calcFunction;
    
    /**
     * Constructor that takes filter value
     * @param filterValue
     */
    public NotEqualsMeasureFilterImpl(double filterValue,int index,MolapCalcFunction calcFunction)
    {
        this.filterValue = filterValue;
        this.index = index;
        this.calcFunction = calcFunction;
    }

    /**
     * See interface commnets.
     * @param filterValue
     */
    @Override
    public boolean filter(MeasureAggregator[] msrValue)
    {
        if(calcFunction != null)
        {
            return calcFunction.calculate(msrValue) != filterValue;
        }
        return msrValue[index].getValue() != filterValue;
    }
    
    /**
     * See interface commnets.
     * @param filterValue
     */
    @Override
    public boolean filter(double[] msrValue,int msrStartIndex)
    {
        return msrValue[index+msrStartIndex] != filterValue;
    }

}
