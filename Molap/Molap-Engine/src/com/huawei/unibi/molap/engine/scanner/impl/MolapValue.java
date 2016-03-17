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

/**
 * 
 */
package com.huawei.unibi.molap.engine.scanner.impl;

import java.io.Serializable;
import java.util.Arrays;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;

/**
 * @author R00900208
 *
 */
public class MolapValue implements Serializable,Comparable<MolapValue>
{

    /**
     * 
     */
    private static final long serialVersionUID = 8034398963696130423L;
    
    private MeasureAggregator[] values;
    
    private int topNIndex;

    public MolapValue(MeasureAggregator[] values)
    {
        this.values = values;
    }

    /**
     * @return the values
     */
    public MeasureAggregator[] getValues()
    {
        return values;
    }
    
    public MolapValue merge(MolapValue another)
    {
        for(int i = 0;i < values.length;i++)
        {
            values[i].merge(another.values[i]);
        }
        return this;
    }
    
    public void setTopNIndex(int index)
    {
        this.topNIndex = index;
    }
    
    public void addGroup(MolapKey key,MolapValue value)
    {
        
    }
    
    public MolapValue mergeKeyVal(MolapValue another)
    {
        return another;
    }
    
    @Override
    public String toString()
    {
        return Arrays.toString(values);
    }

    @Override
    public int compareTo(MolapValue o)
    {
        return values[topNIndex].compareTo(o.values[topNIndex]);
    }
    

}
