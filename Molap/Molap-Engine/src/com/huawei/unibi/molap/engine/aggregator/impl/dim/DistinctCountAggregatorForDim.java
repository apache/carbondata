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
package com.huawei.unibi.molap.engine.aggregator.impl.dim;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashSet;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.dataholder.MolapReadDataHolder;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;

/**
 * Distinct aggregator for dimensions
 * @author R00900208
 *
 */
public class DistinctCountAggregatorForDim implements MeasureAggregator
{
    /**
     * 
     */
    private static final long serialVersionUID = 1860704158506108621L;
    
    private HashSet<ByteArrayWrapper> valueSetForDim = new HashSet<ByteArrayWrapper>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    
    private byte[] maxKey;
    
    private int[] maskByteRanges;
    
    private int maskByteLen;
    
    
    /**
     * For Spark MOLAP to avoid heavy object transfer it better to flatten the Aggregators. There is no aggregation expected after setting this value.  
     */
    private Double computedFixedValue;
    
    public DistinctCountAggregatorForDim(byte[] maxKey, int[] maskByteRanges) 
    {
    	this.maxKey = maxKey;
    	this.maskByteRanges = maskByteRanges;
    	maskByteLen = maskByteRanges.length;
    }
    
    public DistinctCountAggregatorForDim()
    {
    	
    }
    
    @Override
    public int compareTo(MeasureAggregator msrAggrInfo) 
    {
        double val = getDoubleValue();
        double otherVal = msrAggrInfo.getDoubleValue();
        if(val > otherVal)
        {
            return 1;
        }
        if(val < otherVal)
        {
            return -1;
        }
        return 0;
    }

    @Override
    public void agg(double newVal)
    	{
       
    }

    @Override
    public void agg(Object newVal)
    {
     
        }

    @Override
    public void agg(MolapReadDataHolder newVal, int index)
    {

    }

    @Override
    public byte[] getByteArray()
    {
        return null;
    }


    @Override
    public Double getDoubleValue()
    {
        if(computedFixedValue == null)
        {
            return (double)valueSetForDim.size();
        }
        return computedFixedValue;
    }
        
    @Override
    public Long getLongValue()
    {
        if(computedFixedValue == null)
        {
            return (long)valueSetForDim.size();
        }
        return computedFixedValue.longValue();
    }

    @Override
    public BigDecimal getBigDecimalValue()
    {
        if(computedFixedValue == null)
        {
            return new BigDecimal(valueSetForDim.size());
        }
        return new BigDecimal(computedFixedValue);
    }

    @Override
    public Object getValueObject()
    {
        return valueSetForDim.size();
    }

    @Override
    public void merge(MeasureAggregator aggregator)
    {
    	DistinctCountAggregatorForDim countAggregatorForDim = (DistinctCountAggregatorForDim)aggregator;
    	valueSetForDim.addAll(countAggregatorForDim.valueSetForDim);
        
    }

    @Override
    public void setNewValue(Object newValue)
    {
        computedFixedValue = (Double)newValue;
        valueSetForDim = null;
    }

    @Override
    public boolean isFirstTime()
    {
        return false;
    }

    @Override
    public MeasureAggregator getCopy()
    {
    	DistinctCountAggregatorForDim countAggregatorForDim = new DistinctCountAggregatorForDim();
    	countAggregatorForDim.valueSetForDim = new HashSet<ByteArrayWrapper>(valueSetForDim);
    	countAggregatorForDim.maskByteLen = maskByteLen;
    	countAggregatorForDim.maskByteRanges = maskByteRanges.clone();
    	countAggregatorForDim.maxKey = maxKey.clone();
    	return countAggregatorForDim;
    }

    @Override
    public void writeData(DataOutput output) throws IOException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void readData(DataInput inPut) throws IOException
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public MeasureAggregator get()
    {
        return this;
    }

    @Override
    public void merge(byte[] value)
    {
        // TODO Auto-generated method stub
    }

}
