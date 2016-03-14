package com.huawei.unibi.molap.engine.aggregator.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.roaringbitmap.RoaringBitmap;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
/**
 * @author K00900841
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
public class SurrogateBasedDistinctCountAggregator implements MeasureAggregator
{
    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 6313463368629960186L;

    private static final LogService LOGGER = LogServiceFactory.getLogService(SurrogateBasedDistinctCountAggregator.class.getName());
    /**
     * bitSet
     */
    private RoaringBitmap bitSet;
    
    /**
     * For Spark MOLAP to avoid heavy object transfer it better to flatten the Aggregators. There is no aggregation expected after setting this value.  
     */
    private Double computedFixedValue;
    
    private byte[] data;

    /**
     * SurrogateBasedDistinctCountAggregator
     */
    public SurrogateBasedDistinctCountAggregator()
    {
        bitSet = new RoaringBitmap();
    }

    /**
     * just need to add the unique values to agg set
     */
    @Override
    public void agg(double newVal, byte[] key, int offset, int length)
    {//CHECKSTYLE:OFF    Approval No:Approval-V3R8C00_013
        int a = (int)newVal;//CHECKSTYLE:ON
        bitSet.add(a);
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
        { //CHECKSTYLE:ON  //CHECKSTYLE:OFF    Approval No:Approval-V3R8C00_013
            bitSet.add((int)buffer.getDouble());//CHECKSTYLE:ON
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

    private void agg(RoaringBitmap bitSet2)
    {
        bitSet.or(bitSet2);
    }

    /**
     * merge the valueset so that we get the count of unique values
     */
    @Override
    public void merge(MeasureAggregator aggregator)
    {
        SurrogateBasedDistinctCountAggregator distinctCountAggregator = (SurrogateBasedDistinctCountAggregator)aggregator;
        readData();
        distinctCountAggregator.readData();
        agg(distinctCountAggregator.bitSet);
    }

    @Override
    public double getValue()
    {
        if(computedFixedValue == null)
        {
            readData();
            return bitSet.getCardinality();
        }
        return computedFixedValue;
    }

    @Override
    public Object getValueObject()
    {
        return bitSet.getCardinality();
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
        bitSet = null;
    }

    @Override
    public boolean isFirstTime()
    {
        return false;
    }

    @Override
    public void writeData(DataOutput output) throws IOException
    {
        bitSet.serialize(output);
    }

    @Override
    public void readData(DataInput inPut) throws IOException
    {
        bitSet = new RoaringBitmap();
        bitSet.deserialize(inPut);
    }

    @Override
    public MeasureAggregator getCopy()
    {
        SurrogateBasedDistinctCountAggregator aggregator = new SurrogateBasedDistinctCountAggregator();
        aggregator.bitSet= bitSet.clone();       
        return aggregator;
    }

    //we are not comparing the Aggregator values 
    /* public boolean equals(MeasureAggregator msrAggregator){
         return compareTo(msrAggregator)==0;
     }*/
     
    
    @Override
    public int compareTo(MeasureAggregator object)
    {
        double val = getValue();
        double otherVal = object.getValue();
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
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(stream);
        try
        {
            writeData(outputStream);
        }
        catch(IOException e)
        {
//            e.printStackTrace();
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, e.getMessage());
        }
        data = stream.toByteArray();
        bitSet = null;
        return this;
    }
    
    private void readData()
    {
        if(bitSet == null || bitSet.isEmpty())
        {
            ByteArrayInputStream stream = new ByteArrayInputStream(data);
            DataInputStream outputStream = new DataInputStream(stream);
            try
            {
                readData(outputStream);
                outputStream.close();
            }
            catch(IOException e)
            {
                // TODO Auto-generated catch block
//                e.printStackTrace();
                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, e.getMessage());
            }
        }
    }

    @Override
    public void merge(byte[] value)
    {
        // TODO Auto-generated method stub
        
    }
}