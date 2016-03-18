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

/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcz8AOhvEHjQfa55oxvUSJWRQCwLl+VwWEHaV7n
0eFj3bM8lrDwWIVie6WzFr62URr9ic09ekKlU1tINy2Xg+N1t72IA2o1go3BZ+xR71LQH+Gz
Dnd9aGDVgDLREUlGNZWa+KrmQW7QaEBO9zbt+lFwTWG9svSc8BSm+W4/1EgITA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.filters.measurefilter;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;

/**
 * It is "OR" measure group, so any one condition is true then it is ok.
 * @author R00900208
 *
 */
public class OrMeasureGroupFilterImpl implements MeasureFilter,MeasureGroupFilter
{
    
    private MeasureFilter[][] measureFilters;
    
    private int[] msrFilterIndices;

    public OrMeasureGroupFilterImpl(MeasureFilter[][] measureFilters)
    {
        msrFilterIndices = MeasureFilterUtil.getMsrFilterIndexes(measureFilters);
        this.measureFilters = measureFilters;
    }

    @Override
    public boolean filter(MeasureAggregator[] msrValue)
    {
        for(int i = 0;i < msrFilterIndices.length;i++)
        {
            if(measureFilters[msrFilterIndices[i]][0].filter(msrValue))
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean filter(double[] msrValue,int msrStartIndex)
    {
        for(int i = 0;i < msrFilterIndices.length;i++)
        {
            if(measureFilters[msrFilterIndices[i]][0].filter(msrValue,msrStartIndex))
            {
                return true;
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
