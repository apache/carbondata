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
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;

public class DistinctStringCountAggregator implements MeasureAggregator
{
    private static final long serialVersionUID = 6313463368629960186L;

    private Set<String> valueSet;

    public DistinctStringCountAggregator()
    {
        this.valueSet = new HashSet<String>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    }

    public void agg(double newVal, byte[] key, int offset, int length)
    {
    }

    public void agg(String newVal)
    {
        this.valueSet.add(newVal);
    }

    public void agg(double newVal, double factCount)
    {
    }

    private void agg(Set<String> set2)
    {
        this.valueSet.addAll(set2);
    }

    public void merge(MeasureAggregator aggregator)
    {
        DistinctStringCountAggregator distinctCountAggregator = (DistinctStringCountAggregator)aggregator;
        agg(distinctCountAggregator.valueSet);
    }

    public double getValue()
    {
        return this.valueSet.size();
    }

    public Object getValueObject()
    {
        return Integer.valueOf(this.valueSet.size());
    }

    public void setNewValue(double newValue)
    {
    }

    public boolean isFirstTime()
    {
        return false;
    }

    public void writeData(DataOutput output) throws IOException
    {
        int length = this.valueSet.size() * 8;
        ByteBuffer byteBuffer = ByteBuffer.allocate(length + 4);
        byteBuffer.putInt(length);
        for(String val : this.valueSet)
        {
            byte[] b = val.getBytes(Charset.defaultCharset());
            byteBuffer.putInt(b.length);
            byteBuffer.put(b);
        }
        byteBuffer.flip();
        output.write(byteBuffer.array());
    }

    public void readData(DataInput inPut) throws IOException
    {
        int length = inPut.readInt();
        length /= 8;
        this.valueSet = new HashSet<String>(length + 1, 1.0F);
        for(int i = 0;i < length;i++)
        {
            byte[] b = new byte[inPut.readInt()];
            inPut.readFully(b);
            this.valueSet.add(new String(b, Charset.defaultCharset()));
        }
    }

    public MeasureAggregator getCopy()
    {
        DistinctStringCountAggregator aggregator = new DistinctStringCountAggregator();
        aggregator.valueSet = new HashSet<String>(this.valueSet);
        return aggregator;
    }
    
    //we are not comparing the Aggregator values 
    /* public boolean equals(MeasureAggregator msrAggregator){
         return compareTo(msrAggregator)==0;
     }*/
     
    public int compareTo(MeasureAggregator o)
    {
        double val = getValue();
        double otherVal = o.getValue();
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
    public void agg(Object newVal, byte[] key, int offset, int length)
    {
        this.valueSet.add((String)newVal);
    }

    @Override
    public byte[] getByteArray()
    {
        return null;
    }

    @Override
    public MeasureAggregator get()
    {
        return this;
    }
    
    public String toString()
    {
        return valueSet.size()+"";
    }

    @Override
    public void merge(byte[] value)
    {
        // TODO Auto-generated method stub
        
    }

}
