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

package com.huawei.unibi.molap.engine.aggregator.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;

/**
 * @author K00900207
 * 
 *         <p>
 *         The sum distinct aggregator
 *         <p>
 *         Ex:
 *         <p>
 *         ID NAME Sales
 *         <p>
 *         1 a 200
 *         <p>
 *         2 a 100
 *         <p>
 *         3 a 200
 *         <p>
 *         select sum(distinct sales) # would result 300
 */
/**
 * @author K00900207
 *
 */
/**
 * @author K00900207
 *
 */
public class SumDistinctAggregator implements MeasureAggregator
{
    
    /**
     * 
     */
    private static final long serialVersionUID = 6313463368629960155L;
    
    /**
     * For Spark MOLAP to avoid heavy object transfer it better to flatten the
     * Aggregators. There is no aggregation expected after setting this value.
     */
    private Double computedFixedValue;

   

    /**
     * 
     */
    private Set<Double> valueSet;

    public SumDistinctAggregator()
    {
        valueSet = new HashSet<Double>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    }

    /**
     * just need to add the unique values to agg set
     */
    @Override
    public void agg(double newVal, byte[] key, int offset, int length)
    {
        valueSet.add(newVal);
    }

    /**
     * Distinct Aggregate function which update the Distinct set
     * 
     * @param newVal
     *            new value
     * @param key
     *            mdkey
     * @param offset
     *            key offset
     * @param length
     *            length to be considered
     * 
     */
    @Override
    public void agg(Object newVal, byte[] key, int offset, int length)
    {
        valueSet.add(newVal instanceof Double ? (Double)newVal : new Double(newVal.toString()));
    }

    /**
     * Below method will be used to get the value byte array
     */
    @Override
    public byte[] getByteArray()
    {
        Iterator<Double> iterator = valueSet.iterator();
        ByteBuffer buffer = ByteBuffer.allocate(valueSet.size() * MolapCommonConstants.DOUBLE_SIZE_IN_BYTE);
        // CHECKSTYLE:OFF Approval No:Approval-V3R8C00_018
        while(iterator.hasNext())
        { // CHECKSTYLE:ON
            buffer.putDouble(iterator.next());
        }
        buffer.rewind();
        return buffer.array();
    }

    @Override
    public void agg(double newVal, double factCount)
    {
        valueSet.add(newVal);
    }

    private void agg(Set<Double> set2)
    {
        valueSet.addAll(set2);
    }

    /**
     * merge the valueset so that we get the count of unique values
     */
    @Override
    public void merge(MeasureAggregator aggregator)
    {
        SumDistinctAggregator distinctAggregator = (SumDistinctAggregator)aggregator;
        agg(distinctAggregator.valueSet);
    }

    @Override
    public double getValue()
    {
        if(computedFixedValue == null)
        {
            double result = 0;
            for(Double aValue : valueSet)
            {
                result += aValue;
            }
            return result;
        }
        return computedFixedValue;
    }

    @Override
    public Object getValueObject()
    {
        return getValue();
    }

    /**
     * 
     * @see com.huawei.unibi.molap.engine.aggregator.MeasureAggregator#setNewValue(double)
     * 
     */
    @Override
    public void setNewValue(double newValue)
    {
        computedFixedValue = newValue;
        valueSet = null;
    }

    @Override
    public boolean isFirstTime()
    {
        return false;
    }

    @Override
    public void writeData(DataOutput dataOutput) throws IOException
    {
        if(computedFixedValue != null)
        {
            ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 8);
            byteBuffer.putInt(-1);
            byteBuffer.putDouble(computedFixedValue);
            byteBuffer.flip();
            dataOutput.write(byteBuffer.array());
        }
        else
        {
            int length = valueSet.size() * 8;
            ByteBuffer byteBuffer = ByteBuffer.allocate(length + 4 + 1);
            byteBuffer.putInt(length);
            for(double val : valueSet)
            {
                byteBuffer.putDouble(val);
            }
            byteBuffer.flip();
            dataOutput.write(byteBuffer.array());
        }
    }

  
    @Override
    public MeasureAggregator get()
    {
        return this;
    }

    public String toString()
    {
        if(computedFixedValue == null)
        {
            return valueSet.size() + "";
        }
        return computedFixedValue + "";
    }

    @Override
    public void merge(byte[] value)
    {
        if(0 == value.length)
        {
            return;
        }
        ByteBuffer buffer = ByteBuffer.wrap(value);
        buffer.rewind();
        while(buffer.hasRemaining())
        {
            agg(buffer.getDouble(), null, 0, 0);
        }
    }
    
    @Override
    public MeasureAggregator getCopy()
    {
        SumDistinctAggregator aggregator = new SumDistinctAggregator();
        aggregator.valueSet = new HashSet<Double>(valueSet);
        return aggregator;
    }

    @Override
    public int compareTo(MeasureAggregator msr)
    {
        double msrValObj = getValue(); 
        double otherVal = msr.getValue();
        if(msrValObj > otherVal)
        {
            return 1;
        }
        if(msrValObj < otherVal)
        {
            return -1;
        }
        return 0;
    }
    
    @Override
    public void readData(DataInput inPut) throws IOException
    {
        int length = inPut.readInt();

        if(length == -1)
        {
            computedFixedValue = inPut.readDouble();
            valueSet = null;
        }
        else
        {
            length = length / 8;
            valueSet = new HashSet<Double>(length + 1, 1.0f);
            for(int i = 0;i < length;i++)
            {
                valueSet.add(inPut.readDouble());
            }
        }

    }

}
