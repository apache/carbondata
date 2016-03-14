/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcz8AOhvEHjQfa55oxvUSJWRQCwLl+VwWEHaV7n
0eFj3cKbFRF5tDcQiMr2DCiXF0VJ0vgdrWpoZwALAIBrwg1yJAkU5fyN+hRtOlSj6xubIvz3
eoYin4HD5rJWOePlKJxvq4sUwPXjD+zvxONTfqBw7f4j7ehjVtW+HXifw8s57Q==*/
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
 * EqualsMeasureFilter
 * @author R00900208
 *
 */
public class EqualsMeasureFilterImpl implements MeasureFilter 
{

    private double filterValue;
    
    private int index;
    
    private MolapCalcFunction calcFunction;
    
    /**
     * Constructor that takes filter value
     * @param filterValue
     */
    public EqualsMeasureFilterImpl(double filterValue,int index,MolapCalcFunction calcFunction)
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
            return 0 == Double.compare(calcFunction.calculate(msrValue), filterValue) ? true : false;            
        }
        return 0 == Double.compare(msrValue[index].getValue(), filterValue) ? true : false;
    }

    /**
     * See interface commnets.
     * @param filterValue
     */   
    @Override
    public boolean filter(double[] msrValue,int msrStartIndex)
    {
        return 0 == Double.compare(msrValue[index+msrStartIndex], filterValue) ? true : false;
    }
    
}
