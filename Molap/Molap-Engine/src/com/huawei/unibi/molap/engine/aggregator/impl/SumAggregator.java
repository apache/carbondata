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
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdEVzw1icjfRowqz2DW4XzUpEhhSzBOwVynEHjc
u0090fbnj+0VbOfZnjQdUjNGeZBp/OEV/ihcZz/8Bj30H6cdHtnLWokryD8YEIDSBoqj0HMv
x2bWOm2rwPhsF8R5ByGyW4CmQm6QiI4mdcr/+CnCQ2iadvOiXEuMjfxMA+hszQ==*/
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
package com.huawei.unibi.molap.engine.aggregator.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;

/**
 * Project Name NSE V3R7C00
 * 
 * Module Name : Molap Engine
 * 
 * Author K00900841
 * 
 * Created Date :13-May-2013 3:35:33 PM
 * 
 * FileName : SumAggregator.java
 * 
 * Class Description : It will return sum of all the values
 * 
 * Version 1.0
 */
public class SumAggregator implements MeasureAggregator
{

    /**
     * serialVersionUID
     * 
     */
    private static final long serialVersionUID = 623750056131364540L;

    /**
     * aggregate value
     */
    private double aggVal;
    
    /**
     * 
     */
    private boolean firstTime = true;

    /**
     * This method will update the aggVal it will add new value to aggVal
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
    public void agg(double newVal, byte[] key, int offset, int length)
    {
        aggVal += newVal;
        firstTime = false;
    }

    /**
     * This method will update the aggVal it will add new value to aggVal
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
        aggVal += (Double)newVal;
        firstTime = false;
    }

    /**
     * Below method will be used to get the value byte array
     */
    @Override
    public byte[] getByteArray()
    {
        if(firstTime)
        {
            return new byte[0];
        }
        ByteBuffer buffer = ByteBuffer.allocate(MolapCommonConstants.DOUBLE_SIZE_IN_BYTE);
        buffer.putDouble(aggVal);
        return buffer.array();
    }
    
    /**
     * This method will return aggVal
     * 
     * @return sum value
     * 
     */

    @Override
    public double getValue()
    {
        return aggVal;
    }

    /**
     * Merge the value, it will update the sum aggregate value it will add new
     * value to aggVal
     * 
     * @param aggregator
     *            SumAggregator
     * 
     */
    @Override
    public void merge(MeasureAggregator aggregator)
    {
        if(!aggregator.isFirstTime())
        {
            agg(aggregator.getValue(), null, 0, 0);
        }
    }

    /**
     * Overloaded Aggregate function will be used for Aggregate tables because
     * aggregate table will have fact_count as a measure. it will add new value
     * to aggVal
     * 
     * @param newVal
     *            new value
     * @param factCount
     *            total fact count
     * 
     */

    @Override
    public void agg(double newVal, double factCount)
    {
        agg(newVal, null, 0, 0);
    }

    /**
     * This method return the sum value as an object
     * 
     * @return sum value as an object
     */
    @Override
    public Object getValueObject()
    {
        return aggVal;
    }

    /**
     * 
     * @see com.huawei.unibi.molap.engine.aggregator.MeasureAggregator#setNewValue(double)
     * 
     */
    @Override
    public void setNewValue(double newValue)
    {
        aggVal = newValue;
    }

    @Override
    public boolean isFirstTime()
    {
        return firstTime;
    }
    
    //TODO SIMIAN
    @Override
    public void readData(DataInput inPut) throws IOException
    {
        firstTime = inPut.readBoolean();
        aggVal = inPut.readDouble();
    }

    @Override
    public void writeData(DataOutput output) throws IOException
    {
        output.writeBoolean(firstTime);
        output.writeDouble(aggVal);
        
    }

  
    @Override
    public MeasureAggregator getCopy() 
    {
        SumAggregator aggr = new SumAggregator();
        aggr.aggVal = aggVal;
        aggr.firstTime = firstTime;
        return aggr; 
    }
    
    //we are not comparing the Aggregator values 
    /* public boolean equals(MeasureAggregator msrAggregator){
         return compareTo(msrAggregator)==0;
     }*/
     

    @Override
    public int compareTo(MeasureAggregator o)
    {
        double value = getValue();
        double otherVal = o.getValue();
        if(value > otherVal)
        {
            return 1;
        }
        if(value < otherVal)
        {
            return -1;
        }
        return 0;
    }

    

    @Override
    public void merge(byte[] value)
    {
        if(0 == value.length)
        {
            return;
        }
        aggVal+= ByteBuffer.wrap(value).getDouble();
        firstTime = false;
    }
    
    @Override
    public MeasureAggregator get()
    {
        return this;
    }
    
    public String toString()
    {
        return aggVal+"";
    }
}
