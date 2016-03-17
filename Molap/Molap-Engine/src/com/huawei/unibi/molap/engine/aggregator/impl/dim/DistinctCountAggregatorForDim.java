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
import java.util.HashSet;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
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
    
    private HashSet<ByteArrayWrapper> valueSet = new HashSet<ByteArrayWrapper>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    
    private byte[] maxKey;
    
    private int[] maskByteRanges;
    
    private int maskByteLen;
    
    private ByteArrayWrapper wrapper;
    
    /**
     * For Spark MOLAP to avoid heavy object transfer it better to flatten the Aggregators. There is no aggregation expected after setting this value.  
     */
    private Double computedFixedValue;
    
    public DistinctCountAggregatorForDim(byte[] maxKey, int[] maskByteRanges) 
    {
    	this.maxKey = maxKey;
    	this.maskByteRanges = maskByteRanges;
    	maskByteLen = maskByteRanges.length;
    	wrapper = new ByteArrayWrapper();
    }
    
    public DistinctCountAggregatorForDim()
    {
    	
    }
    
    //we are not comparing the Aggregator values 
    /* public boolean equals(MeasureAggregator msrAggregator){
         return compareTo(msrAggregator)==0;
     }*/
     
    
    
    //TODO SIMIAN
    @Override
    public int compareTo(MeasureAggregator msrAggrInfo) 
    {
        double val = getValue();
        double otherVal = msrAggrInfo.getValue();
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
    public void agg(double newVal, byte[] key, int offset, int length)
    {
    	wrapper.setData(key, offset, maxKey, maskByteRanges, maskByteLen);
    	if(valueSet.add(wrapper))
    	{
    		wrapper = new ByteArrayWrapper();
    	}
    }

    @Override
    public void agg(Object newVal, byte[] key, int offset, int length)
    {
        wrapper.setData(key, offset, maxKey, maskByteRanges, maskByteLen);
        if(valueSet.add(wrapper))
        {
            wrapper = new ByteArrayWrapper();
        }
    }

    @Override
    public byte[] getByteArray()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void agg(double newVal, double factCount)
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public double getValue()
    {
        if(computedFixedValue == null)
        {
            return valueSet.size();
        }
        return computedFixedValue;
    }

    @Override
    public Object getValueObject()
    {
        return valueSet.size();
    }

    @Override
    public void merge(MeasureAggregator aggregator)
    {
    	DistinctCountAggregatorForDim countAggregatorForDim = (DistinctCountAggregatorForDim)aggregator;
    	valueSet.addAll(countAggregatorForDim.valueSet);
        
    }

    @Override
    public void setNewValue(double newValue)
    {
        computedFixedValue = newValue;
        valueSet = null;
    }

    @Override
    public boolean isFirstTime()
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public MeasureAggregator getCopy()
    {
    	DistinctCountAggregatorForDim countAggregatorForDim = new DistinctCountAggregatorForDim();
    	countAggregatorForDim.valueSet = new HashSet<ByteArrayWrapper>(valueSet);
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
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public void merge(byte[] value)
    {
        // TODO Auto-generated method stub
        
    }

}
