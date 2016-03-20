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

package com.huawei.unibi.molap.engine.filters.measurefilter;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.executer.calcexp.MolapCalcFunction;

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
