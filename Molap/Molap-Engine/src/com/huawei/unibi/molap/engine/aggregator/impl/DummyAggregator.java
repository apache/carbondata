/**
 * 
 */
package com.huawei.unibi.molap.engine.aggregator.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;

/**
 * @author R00903928
 *
 */
public class DummyAggregator implements MeasureAggregator
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    /**
     * aggregate value
     */
    private double aggVal;

    @Override
    public int compareTo(MeasureAggregator o)
    {
        if(equals(o))
        {
            return 0;
        }
        return -1;
    }
    @Override
    public boolean equals(Object arg0)
    {
        return super.equals(arg0);
    }
    @Override
    public int hashCode()
    {
        return super.hashCode();
    }
    @Override
    public void agg(double newVal, byte[] key, int offset, int length)
    {
        aggVal = newVal;
    }

    @Override
    public void agg(Object newVal, byte[] key, int offset, int length)
    {
       
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
        aggVal =(Double) newVal;
    }

    @Override
    public double getValue()
    {
        return aggVal;
    }

    @Override
    public Object getValueObject()
    {
        // TODO Auto-generated method stub
        return aggVal;
    }

    @Override
    public void merge(MeasureAggregator aggregator)
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setNewValue(double newValue)
    {
        // TODO Auto-generated method stub
        aggVal = newValue;
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
        // TODO Auto-generated method stub
        return null;
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
