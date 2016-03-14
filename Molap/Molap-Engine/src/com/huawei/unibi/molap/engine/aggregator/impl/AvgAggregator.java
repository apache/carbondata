/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdEVzw1icjfRowqz2DW4XzUpEhhSzBOwVynEHjc
u0090YeyNJjyiBxlZZhvq198q+Px/O6umGvGwr5h9OKhpMctsfEvwH0Ku71ImcKU6VAJ7mHZ
e2xQU1gqw8DAe8i5OCRnjPMmOC9dX8zPk/kKPGifGLgFauScMSF4Lt2p+I7MLQ==*/
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

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import java.nio.ByteBuffer;

/**
 * Project Name NSE V3R7C00 
 * 
 * Module Name : Molap Engine
 * 
 * Author K00900841
 * 
 * Created Date :13-May-2013 3:35:33 PM
 * 
 * FileName : AvgAggregator.java
 * 
 * Class Description :
 * It will return average of aggregate values
 * 
 * Version 1.0
 */

public class AvgAggregator implements MeasureAggregator
{
    
    /**
     * serialVersionUID
     * 
     */
    private static final long serialVersionUID = 5463736686281089871L;

    /**
     * total number of aggregate values
     */
    protected double count;

    /**
     * aggregate value
     */
    protected double aggVal;
    
    /**
     * 
     */
    protected boolean firstTime = true;

    /**
     * Average Aggregate function which will add all the aggregate values and it
     * will increment the total count every time, for average value
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
        count++;
        firstTime = false;
    }
    
    /**
     * Average Aggregate function which will add all the aggregate values and it
     * will increment the total count every time, for average value
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
        if(newVal instanceof byte[])
        {
            ByteBuffer buffer = ByteBuffer.wrap((byte[])newVal);
            buffer.rewind();
            //CHECKSTYLE:OFF    Approval No:Approval-V3R8C00_018
            while(buffer.hasRemaining())
            { //CHECKSTYLE:ON
               aggVal+=buffer.getDouble();
               count+=buffer.getDouble();
               firstTime = false;
            }            
            return;
        }
        aggVal += (Double)newVal;
        count++;
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
        ByteBuffer buffer = ByteBuffer.allocate(2 * MolapCommonConstants.DOUBLE_SIZE_IN_BYTE);
        buffer.putDouble(aggVal);
        buffer.putDouble(count);
        return buffer.array();
//        return null;
    }

    /**
     * Return the average of the aggregate values
     * 
     * @return average aggregate value
     *
     */
    @Override
    public double getValue()
    {

        return aggVal / count;
    }

    /**
     * This method merge the aggregated value, in average aggregator it will add
     * count and aggregate value
     * 
     * @param aggregator
     *            Avg Aggregator
     * 
     */
    @Override
    public void merge(MeasureAggregator aggregator)
    {
        AvgAggregator avgAggregator = (AvgAggregator)aggregator;
        if(!avgAggregator.isFirstTime())
        {
            aggVal += avgAggregator.aggVal;
            count += avgAggregator.count;
            firstTime = false;
        }
    }

    /**
     * Overloaded Aggregate function will be used for Aggregate tables because
     * aggregate table will have fact_count as a measure.
     * 
     * @param newVal
     *          new value
     * @param factCount
     *          total fact count
     * 
     */
    @Override
    public void agg(double newVal, double factCount)
    {
        aggVal += newVal;
        count += factCount;
        firstTime = false;
    }

    /**
     * This method return the average value as an object
     * 
     * @return average value as an object
     */
    @Override
    public Object getValueObject()
    {
        // TODO Auto-generated method stub
        return aggVal / count;
    }

    /**
     * 
     * @see com.huawei.unibi.molap.engine.aggregator.MeasureAggregator#setNewValue(double)
     * 
     */
    @Override
    public void setNewValue(double newValue)
    {
        aggVal=newValue;
        count = 1;
    }

    @Override
    public boolean isFirstTime()
    {
        return firstTime;
    }

    @Override
    public void writeData(DataOutput output) throws IOException
    {
        output.writeBoolean(firstTime);
        output.writeDouble(aggVal);
        output.writeDouble(count);
        
    }

    @Override
    public void readData(DataInput inPut) throws IOException
    {
        firstTime = inPut.readBoolean();
        aggVal = inPut.readDouble();
        count = inPut.readDouble();
    }

    @Override
    public MeasureAggregator getCopy()
    {
        AvgAggregator avg = new AvgAggregator();
        avg.aggVal = aggVal;
        avg.count = count;
        avg.firstTime = firstTime;
        return avg;
    }

    //we are not comparing any Aggregator values
    /*public boolean equals(MeasureAggregator msrAggregator){
        return compareTo(msrAggregator)==0;
    }*/
    
    @Override
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
    public MeasureAggregator get()
    {
        return this;
    }   
    
    public String toString()
    {
        return (aggVal / count)+"";
    }

    @Override
    public void merge(byte[] value)
    {
        if(0 == value.length)
        {
            return;
        }
        ByteBuffer buffer = ByteBuffer.wrap(value);
        aggVal += buffer.getDouble();
        count += buffer.getDouble();
        firstTime = false;
    }

}
