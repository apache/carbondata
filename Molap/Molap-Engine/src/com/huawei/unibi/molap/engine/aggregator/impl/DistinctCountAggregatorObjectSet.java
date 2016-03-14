package com.huawei.unibi.molap.engine.aggregator.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;

public class DistinctCountAggregatorObjectSet implements MeasureAggregator
{

    /**
     * 
     */
    private static final long serialVersionUID = 6313463368629960186L;

    /**
     * 
     */
    private Set<Object> valueSet;

    public DistinctCountAggregatorObjectSet()
    {
        valueSet = new HashSet<Object>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
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
     * Distinct count Aggregate function which update the Distinct count
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
        byte[] values = (byte[])newVal;
        ByteBuffer buffer = ByteBuffer.wrap(values);
        buffer.rewind();
        //CHECKSTYLE:OFF    Approval No:Approval-V3R8C00_018
        while(buffer.hasRemaining())
        { //CHECKSTYLE:ON
            valueSet.add(buffer.getDouble());
        }
    }

    /**
     * Below method will be used to get the value byte array
     */
    @Override
    public byte[] getByteArray()
    {
      return null;
    }

    
    @Override
    public void agg(double newVal, double factCount)
    {
        
    }

    private void agg(Set<Object> set2)
    {
        valueSet.addAll(set2);
    }

    /**
     * merge the valueset so that we get the count of unique values
     */
    @Override
    public void merge(MeasureAggregator aggregator)
    {
        DistinctCountAggregatorObjectSet distinctCountAggregator = (DistinctCountAggregatorObjectSet)aggregator;
        agg(distinctCountAggregator.valueSet);
    }

    @Override
    public double getValue()
    {
        return valueSet.size();
    }

    @Override
    public Object getValueObject()
    {
        return valueSet.size();
    }

    /**
     * 
     * @see com.huawei.unibi.molap.engine.aggregator.MeasureAggregator#setNewValue(double)
     * 
     */
    @Override
    public void setNewValue(double newValue)
    {
    }
    
    /**
     * 
     * @see com.huawei.unibi.molap.engine.aggregator.MeasureAggregator#setNewValue(double)
     * 
     */
    public void setNewValue(Object newValue)
    {
        valueSet.add(newValue);
    }

    @Override
    public boolean isFirstTime()
    {
        return false;
    }

    @Override
    public void writeData(DataOutput output) throws IOException
    {
        
    }

    @Override
    public void readData(DataInput inPut) throws IOException
    {
        
    }

    @Override
    public MeasureAggregator getCopy()
    {

        DistinctCountAggregatorObjectSet aggregator = new DistinctCountAggregatorObjectSet();
        aggregator.valueSet = new HashSet<Object>(valueSet);
        return aggregator;
    }
    
    //we are not comparing the Aggregator values 
    /* public boolean equals(MeasureAggregator msrAggregator){
         return compareTo(msrAggregator)==0;
     }*/
      
    @Override
    public int compareTo(MeasureAggregator measureAggr)
    {
        double valueSetSize = getValue();
        double otherVal = measureAggr.getValue();
        if(valueSetSize > otherVal)
        {
            return 1;
        }
        if(valueSetSize < otherVal)
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
         return valueSet.size()+"";
    }

    @Override
    public void merge(byte[] value)
    {
        // TODO Auto-generated method stub
        
    }

}
