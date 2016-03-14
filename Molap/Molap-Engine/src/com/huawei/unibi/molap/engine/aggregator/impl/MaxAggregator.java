/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdEVzw1icjfRowqz2DW4XzUpEhhSzBOwVynEHjc
u0090RHlGxCT+1p8z4Gk/PhPLTpbv01ABUOToFxKvvSdOeT9RvGF3fvUgSu1ZrA5a/KI2SrV
Co3iitm+TZLyzKWgxmbGqk76EL/M0V/WiPL4QJSgl6vZ1YEI06RObKlXTjDI1g==*/
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
 * FileName : MaxAggregator.java
 * 
 * Class Description :
 * It will return max of values
 * 
 * Version 1.0
 */
public class MaxAggregator implements MeasureAggregator
{

    /**
     * 
     * serialVersionUID
     * 
     */
    private static final long serialVersionUID = -5850218739083899419L;
    /**
     * aggregate value
     */
    private Comparable<Object> aggVal;
    
    /**
     * 
     */
    private boolean firstTime = true;
    
    private static final LogService LOGGER = LogServiceFactory.getLogService(MaxAggregator.class.getName());
    
    /**
     * This method will update the aggVal if aggVal is less than new value
     * 
     * @param newVal
     *          new value
     * @param key
     *          mdkey 
     * @param offset
     *          key offset 
     * @param length
     *          length to be considered 
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
            aggVal = (aggVal==null || aggVal.compareTo(newValue) < 0) ? newValue : aggVal;
        }
    }
    
    /**
     * This method will update the aggVal if aggVal is less than new value
     * 
     * @param newVal
     *          new value
     * @param key
     *          mdkey 
     * @param offset
     *          key offset 
     * @param length
     *          length to be considered 
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
        byte[] objectBytesVal= new byte[0]; 
        if(firstTime)
        {
            return objectBytesVal;
        }
        ByteArrayOutputStream bos =null; 
        ObjectOutput out=null;
        try{
            bos=new ByteArrayOutputStream();
            out = new ObjectOutputStream(bos);

            out.writeObject(aggVal);
            objectBytesVal = bos.toByteArray();
        }
        catch (Exception e) 
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e,
                    "Problem while getting byte array in maxaggregator: " + e.getMessage());
        }
        finally{
           MolapUtil.closeStreams(bos);
        }
        return objectBytesVal;
    }
    
    /**
     * Overloaded Aggregate function will be used for Aggregate tables because
     * aggregate table will have fact_count as a measure. It will update the
     * aggVal if aggVal is less than newVal
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
     * This method will return max value
     * 
     * @return max value
     *
     */
    @Override
    public double getValue()
    {
        return (Double)((Object)aggVal);
    }

    /**
     * Merge the value, it will update the max aggregate value if aggregator
     * passed as an argument will have value greater than aggVal
     * 
     * @param aggregator
     *          MaxAggregator
     *
     */
    @Override
    public void merge(MeasureAggregator aggregator)
    {
        MaxAggregator maxAggregator = (MaxAggregator)aggregator;
//        if(!maxAggregator.isFirstTime())
//        {
            agg(maxAggregator.aggVal, null, 0, 0);
//        }
    }

    /**
     * This method return the max value as an object
     * 
     * @return max value as an object
     */
    @Override
    public Object getValueObject()
    {
        return aggVal;
    }

      
    @Override
    public void writeData(DataOutput dataOutput) throws IOException 
    {
        ByteArrayOutputStream bos = null;
        ObjectOutput out = null;
        
        try 
        {
            dataOutput.writeBoolean(firstTime);
            bos=new ByteArrayOutputStream();
            out= new ObjectOutputStream(bos);
            out.writeObject(aggVal);
            byte[] objectBytes = bos.toByteArray();
            dataOutput.write(objectBytes.length);
            dataOutput.write(objectBytes, 0, objectBytes.length);
        }catch(Exception e){
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e,
                    "Problem while getting byte array in maxaggregator: " + e.getMessage());

        }finally{
            MolapUtil.closeStreams(bos);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readData(DataInput inPut) throws IOException
    {
       
        ByteArrayInputStream bis = null;
        ObjectInput in =null; 
        try 
        {
            firstTime = inPut.readBoolean();
            int len = inPut.readInt(); 
            byte[] data = new byte[len];
            bis=new ByteArrayInputStream(data);
            in=new ObjectInputStream(bis);
            aggVal = (Comparable<Object>)in.readObject();
        }
        catch(Exception e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e,
                    "Problem while getting byte array in maxaggregator: " + e.getMessage());
        }finally{
            MolapUtil.closeStreams(bis);
            
        }
    }

    @Override
    public MeasureAggregator getCopy()
    {
        MaxAggregator aggregator = new MaxAggregator();
        aggregator.aggVal = aggVal;
        aggregator.firstTime = firstTime;
        return aggregator;
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

    @Override
    public boolean isFirstTime()
    {
        return firstTime;
    }
    
    @Override 
    public MeasureAggregator get()
    {
        return this;

    }
    
    @Override
    public int compareTo(MeasureAggregator msrAggr)
    {
        @SuppressWarnings("unchecked")
        Comparable<Object> other = (Comparable<Object>)msrAggr.getValueObject();
        
        return aggVal.compareTo(other);
    }
    
    public String toString()
    {
        return aggVal+"";
    }

    @Override
    public void merge(byte[] value)
    {
        if(0 == value.length)
        {
            return;
        }
        ByteArrayInputStream bytesInputStream = null;  
        ObjectInput in =null; 
        try 
        {
            bytesInputStream=new ByteArrayInputStream(value);
            in = new ObjectInputStream(bytesInputStream);
            Object newVal = (Comparable<Object>)in.readObject();
            internalAgg(newVal);
            firstTime = false;
        }
        catch(Exception e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e,
                    "Problem while merging byte array in maxaggregator: " + e.getMessage());
        }finally{
            MolapUtil.closeStreams(bytesInputStream);
        }
    }
}
