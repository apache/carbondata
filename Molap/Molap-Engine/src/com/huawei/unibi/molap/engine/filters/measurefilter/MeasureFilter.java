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
