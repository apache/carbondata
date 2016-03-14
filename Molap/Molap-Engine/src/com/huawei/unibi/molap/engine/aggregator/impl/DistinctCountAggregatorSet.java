/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdEVzw1icjfRowqz2DW4XzUpEhhSzBOwVynEHjc
u0090UJHa7e02Wm6ckFmyt2l30cmd8ICvH9ddJ6R/m4KwZ51eWGldPy6YqvbZ6oNrtr37m75
k1Pi0UWT0KKIOTBn3oMB1+yD/4QA/uhGfRIML6CoQz/RHUqt640EwIN/Z+sOyQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
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
 * @author A00902732
 * 
 *         <p>
 *         The distinct count aggregator
 *         <p>
 *         Ex:
 *         <p>
 *         ID NAME Sales
 *         <p>1 a 200
 *         <p>2 a 100
 *         <p>3 a 200
 *         <p>
 *         select count(distinct sales) # would result 2
 *         <p>
 *         select count(sales) # would result 3
 */
public class DistinctCountAggregatorSet implements MeasureAggregator
{
    /**
     * For Spark MOLAP to avoid heavy object transfer it better to flatten the Aggregators. There is no aggregation expected after setting this value.  
     */
    private Double computedFixedValue;
    
    /**
     * 
     */
    private static final long serialVersionUID = 6313463368629960186L;

    /**
     * 
     */
    private Set<Double> valueSet;

    public DistinctCountAggregatorSet()
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
     * Below method will be used to get the value byte array
     */
    @Override
    public byte[] getByteArray()
    {
        Iterator<Double> iterator = valueSet.iterator();
        ByteBuffer buffer = ByteBuffer.allocate(valueSet.size() * MolapCommonConstants.DOUBLE_SIZE_IN_BYTE);
        //CHECKSTYLE:OFF    Approval No:Approval-V3R8C00_018
        while(iterator.hasNext())
        { //CHECKSTYLE:ON
            buffer.putDouble(iterator.next());
        }
        buffer.rewind();
        return buffer.array();
    }

    //TODO SIMIAN
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
    
    @Override
    public void agg(double newVal, double factCount)
    {
        
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
        DistinctCountAggregatorSet distinctCountAggregator = (DistinctCountAggregatorSet)aggregator;
        agg(distinctCountAggregator.valueSet);
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
    public boolean isFirstTime()
    {
        return false;
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
    public void writeData(DataOutput dataOutputVal) throws IOException
    {
        
        if(computedFixedValue != null)
        {
            ByteBuffer byteBuffer = ByteBuffer.allocate(4+8);
            byteBuffer.putInt(-1);
            byteBuffer.putDouble(computedFixedValue);
            byteBuffer.flip();
            dataOutputVal.write(byteBuffer.array()); 
        }
        else
        {
            int length = valueSet.size()*8;
            ByteBuffer byteBuffer = ByteBuffer.allocate(length+4+1);
            byteBuffer.putInt(length);
            for(double val : valueSet)
            {
                byteBuffer.putDouble(val);
            }
            byteBuffer.flip();
            dataOutputVal.write(byteBuffer.array());
        }
    }

    @Override 
    public void readData(DataInput inPutVal) throws IOException
    {
        int length = inPutVal.readInt();
        
        if(length ==-1)
        {
            computedFixedValue = inPutVal.readDouble();
            valueSet = null;
        }
        else
        {
            length = length/8;
            valueSet = new HashSet<Double>(length+1,1.0f);
            for(int i = 0;i < length;i++)
            {
                valueSet.add(inPutVal.readDouble());
            }
        }
        
    }

    @Override
    public MeasureAggregator getCopy()
    {

        DistinctCountAggregatorSet aggregator = new DistinctCountAggregatorSet();
        aggregator.valueSet = new HashSet<Double>(valueSet);
        return aggregator;
    }
    
    //we are not comparing the Aggregator values 
    /* public boolean equals(MeasureAggregator msrAggregator){
         return compareTo(msrAggregator)==0;
     }*/
    
    public String toString()
    {
        if(computedFixedValue == null)
        {
            return valueSet.size()+"";
        }
        return computedFixedValue+"";
    }
    
    @Override
    public int compareTo(MeasureAggregator msr)
    {
        double msrVal = getValue(); 
        double otherMsrVal = msr.getValue();    
        if(msrVal > otherMsrVal)
        {
            return 1;
        }
        if(msrVal < otherMsrVal)
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
    
   
    @Override
    public void merge(byte[] value)
    {
        // TODO Auto-generated method stub
        
    }

}
