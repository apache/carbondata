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
