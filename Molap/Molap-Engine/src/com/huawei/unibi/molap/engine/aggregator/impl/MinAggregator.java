/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdEVzw1icjfRowqz2DW4XzUpEhhSzBOwVynEHjc
u0090X053sNIn6xK06q3gCqt2vQZyHdjWh4EgyEk3/NENKwBZ1UhJrk+bQFxX+d8etuJdplF
NCPDfMiTrvF0I3f4ENm4EsZqBL1hakCwUGR384EWcdhbBo/fYpQ5wfMeFuWJ5w==*/
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * Project Name NSE V3R7C00
 * 
 * Module Name : Molap Engine
 * 
 * Author K00900841
 * 
 * Created Date :13-May-2013 3:35:33 PM
 * 
 * FileName : MinAggregator.java
 * 
 * Class Description : It will return min of values
 * 
 * Version 1.0
 */
public class MinAggregator implements MeasureAggregator
{

    /**
     * 
     * serialVersionUID
     * 
     */
    private static final long serialVersionUID = -8077547753784906280L;

    /**
     * aggregate value
     */
    private Comparable<Object> aggVal;
    
    /**
     * 
     */
    private boolean firstTime = true;
    
    private static final LogService LOGGER = LogServiceFactory.getLogService(MinAggregator.class.getName());

    /**
     * This method will update the min of values if new value is less than
     * aggVal
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
        internalAgg((Double)newVal);
        firstTime = false;
    }

    private void internalAgg(Object value)
    {
        if(value instanceof Comparable)
        {
            @SuppressWarnings("unchecked")
            Comparable<Object> newValue = ((Comparable<Object>)value);
            aggVal = (aggVal==null || aggVal.compareTo(newValue) > 0) ? newValue : aggVal;
        }
    }
    
    /**
     * This method will update the min of values if new value is less than
     * aggVal
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
        internalAgg(newVal);
        firstTime = false;
    }

    /**
     * Below method will be used to get the value byte array
     */
    @Override
    public byte[] getByteArray()
    {
        byte[] objectBytes= new byte[0];
        if(firstTime)
        {
            return objectBytes;
        }
        ObjectOutput out = null;
        ByteArrayOutputStream bos = null;
        try 
        {
            bos=new ByteArrayOutputStream();
            out=new ObjectOutputStream(bos);
            out.writeObject(aggVal);
            objectBytes = bos.toByteArray();
        }
        catch (IOException e) 
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e,
                    "Problem while getting byte array in minaggregator: " + e.getMessage());
        }finally{
            MolapUtil.closeStreams(bos);
        }
        return objectBytes;
    }
    
    /**
     * This method will return aggVal
     * 
     * @return min value
     * 
     */

    @Override
    public double getValue()
    {
        return (Double)((Object)aggVal);
    }

    /**
     * Merge the value, it will update the min aggregate value if aggregator
     * passed as an argument will have value less than aggVal
     * 
     * @param aggregator
     *            MinAggregator
     * 
     */
    @Override
    public void merge(MeasureAggregator aggregator)
    {
        MinAggregator minAggregator = (MinAggregator)aggregator;
//        if(!minAggregator.isFirstTime())
//        {
            agg(minAggregator.aggVal, null, 0, 0);
//        }
    }

    /**
     * Overloaded Aggregate function will be used for Aggregate tables because
     * aggregate table will have fact_count as a measure. It will update the
     * aggVal if aggVal is greater than newVal
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
        firstTime = false;
    }
    

    /**
     * 
     * @see com.huawei.unibi.molap.engine.aggregator.MeasureAggregator#setNewValue(double)
     * 
     */
    @Override
    public void setNewValue(double newValue)
    {
//        aggVal= newValue;
    }
    

    /**
     * This method return the min value as an object
     * 
     * @return min value as an object
     */
    @Override
    public Object getValueObject()  
    {
        return aggVal;
    }

    @Override
    public boolean isFirstTime()
    {
        return firstTime;
    }
    
    @Override
    public void writeData(DataOutput dataOutputVal1) throws IOException
    {
        ByteArrayOutputStream bos=null;
        ObjectOutput out =null;
        try  
        { 
            dataOutputVal1.writeBoolean(firstTime);
            bos = new ByteArrayOutputStream(); 
            out = new ObjectOutputStream(bos);
            out.writeObject(aggVal);
            byte[] objectBytes = bos.toByteArray();
            dataOutputVal1.write(objectBytes.length);
            dataOutputVal1.write(objectBytes, 0, objectBytes.length);
        }catch(Exception e){

            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e,
                    "Problem while getting byte array in minaggregator: " + e.getMessage());
        }finally{
            MolapUtil.closeStreams(bos);
        }
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
    
    @Override
    public int compareTo(MeasureAggregator o)
    {
        @SuppressWarnings("unchecked")
        Comparable<Object> other = (Comparable<Object>)o.getValueObject();
        
        return aggVal.compareTo(other);
    }    
    

    @SuppressWarnings("unchecked")
    @Override
    public void readData(DataInput inPut) throws IOException
    {
        ByteArrayInputStream bis = null; 
        ObjectInput in = null;
        try 
        {
            int length = inPut.readInt();
            firstTime = inPut.readBoolean();
            byte[] data = new byte[length];
            bis=new ByteArrayInputStream(data);
            in=new ObjectInputStream(bis);
            aggVal = (Comparable<Object>)in.readObject();
        }
        catch(Exception e)
        {

            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e,
                    "Problem while getting byte array in minaggregator: " + e.getMessage());       
         }finally{
            MolapUtil.closeStreams(bis);
        }
    }

    @Override
    public MeasureAggregator getCopy()
    {
        MinAggregator aggregator = new MinAggregator();
        aggregator.aggVal = aggVal;
        aggregator.firstTime = firstTime;
        return aggregator;
    }

    @Override
    public void merge(byte[] value)
    {
        if(0 == value.length)
        {
            return;
        }
        ByteArrayInputStream bis = null; 
        ObjectInput objectInput = null; 
        try 
        {
            bis=new ByteArrayInputStream(value);
            objectInput=new ObjectInputStream(bis);
            Object newVal = (Comparable<Object>)objectInput.readObject();
            internalAgg(newVal);
            firstTime = false;
        }
        catch(Exception e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e,
                    "Problem while merging byte array in minaggregator: " + e.getMessage());
        }finally{
            MolapUtil.closeStreams(bis);
        }
        
    }
}
