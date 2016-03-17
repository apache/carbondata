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
u0090UJHa7e02Wm6ckFmyt2l30cmd8ICvH9ddJ6R/m4KwZ51eWGldPy6YqvbZ6oNrtr37m75
k1Pi0UWT0KKIOTBn3oMB1+yD/4QA/uhGfRIML6CoQz/RHUqt640EwIN/Z+sOyQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.engine.aggregator.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;

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
public class DistinctCountAggregator implements MeasureAggregator
{
    
    private static final LogService LOGGER = LogServiceFactory.getLogService(DistinctCountAggregator.class.getName());
    /**
     * 
     */
    private static final long serialVersionUID = 6313463368629960186L;
    /**
     * For Spark MOLAP to avoid heavy object transfer it better to flatten the Aggregators. There is no aggregation expected after setting this value.  
     */
    private Double computedFixedValue;
    /**
     * 
     */
//    private Set<Double> valueSet;
    private transient RoaringBitmap valueSet;
    
    private byte[] data;
    
    private double minValue;

    public DistinctCountAggregator(double minValue)
    {
        valueSet = new RoaringBitmap();
        this.minValue = minValue;
    }
    
    public DistinctCountAggregator()
    {
        valueSet = new RoaringBitmap();
    }

    /**
     * just need to add the unique values to agg set
     */
    @Override
    public void agg(double newVal, byte[] key, int offset, int length)
    {
        valueSet.add((int)(newVal-minValue));
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
//        byte[] values = (byte[])newVal;
//        ByteArrayInputStream stream = new ByteArrayInputStream(values);
//        DataInputStream outputStream = new DataInputStream(stream);
//        RoaringBitmap bitmap = new RoaringBitmap();
//        try
//        {
//            bitmap.deserialize(outputStream);
//        }
//        catch(IOException e)
//        {
//            e.printStackTrace();
//        }
//        valueSet.or(bitmap);
        byte[] values = (byte[])newVal;
        ByteBuffer buffer = ByteBuffer.wrap(values);
        buffer.rewind();
        //CHECKSTYLE:OFF    Approval No:Approval-V3R8C00_018
        while(buffer.hasRemaining())
        { //CHECKSTYLE:ON
            valueSet.add(buffer.getInt());
        }
    }

    /**
     * Below method will be used to get the value byte array
     */
    @Override
    public byte[] getByteArray()
    {
        if(valueSet.getCardinality()==0)
        {
            return new byte[0];
        }
        IntIterator iterator = valueSet.getIntIterator();
        ByteBuffer buffer = ByteBuffer.allocate(valueSet.getCardinality() * 4 + 8);
        buffer.putDouble(minValue);
        //CHECKSTYLE:OFF    Approval No:Approval-V3R8C00_018
        while(iterator.hasNext())
        { //CHECKSTYLE:ON
            buffer.putInt(iterator.next());
        }
        buffer.rewind();
        return buffer.array();
//        ByteArrayOutputStream bo = new ByteArrayOutputStream();
//        DataOutputStream dos = new DataOutputStream(bo);
//        try
//        {
//            valueSet.serialize(dos);
//            dos.close();
//        }
//        catch(IOException e)
//        {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//        return bo.toByteArray();
    }

    
    @Override
    public void agg(double newVal, double factCount)
    {
        
    }

//    private void agg(Set<Double> set2)
//    {
//        valueSet.addAll(set2);
//    }
    
    private void agg(RoaringBitmap set2, double minValue)
    {
        if(this.minValue == minValue)
        {
            valueSet.or(set2);
        }
        else
        {
            if(this.minValue > minValue)
            {
                IntIterator intIterator = valueSet.getIntIterator();
                while(intIterator.hasNext())
                {
                    set2.add((int)((double)(intIterator.next()+this.minValue)-minValue));
                }
                this.minValue = minValue;
                this.valueSet = set2;
            }
            else
            {
                IntIterator intIterator = set2.getIntIterator();
                while(intIterator.hasNext())
                {
                    valueSet.add((int)((double)(intIterator.next()+minValue)-this.minValue));
                }
            }
        }
    }

    /**
     * merge the valueset so that we get the count of unique values
     */
    @Override
    public void merge(MeasureAggregator aggregator)
    {
        DistinctCountAggregator distinctCountAggregator = (DistinctCountAggregator)aggregator;
        readData();
        distinctCountAggregator.readData();
        if(distinctCountAggregator.valueSet != null)
        {
            agg(distinctCountAggregator.valueSet, distinctCountAggregator.minValue);
        }
    }

    @Override
    public double getValue()
    {
        if(computedFixedValue == null)
        {
            readData();
            return valueSet.getCardinality();
        }
        return computedFixedValue;
    }

    @Override
    public Object getValueObject()
    {
        return valueSet.getCardinality();
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
    public void writeData(DataOutput output) throws IOException
    {
        
        if(computedFixedValue != null)
        {
            ByteBuffer byteBuffer = ByteBuffer.allocate(4+8);
            byteBuffer.putInt(-1);
            byteBuffer.putDouble(computedFixedValue);
            byteBuffer.flip();
            output.write(byteBuffer.array());
        }
        else
        {
//            int length = valueSet.size()*8;
//            ByteBuffer byteBuffer = ByteBuffer.allocate(length+4+1);
//            byteBuffer.putInt(length);
//            for(double val : valueSet)
//            {
//                byteBuffer.putDouble(val);
//            }
//            byteBuffer.flip();
//            output.write(byteBuffer.array());
            if(valueSet != null) {
                valueSet.serialize(output);
            } else {
                output.write(data);
            }
        }
    }

    @Override
    public void readData(DataInput inPut) throws IOException
    {
//        int length = inPut.readInt();
//        
//        if(length ==-1)
//        {
//            computedFixedValue = inPut.readDouble();
//            valueSet = null;
//        }
//        else
//        {
//            length = length/8;
//            valueSet = new HashSet<Double>(length+1,1.0f);
//            for(int i = 0;i < length;i++)
//            {
//                valueSet.add(inPut.readDouble());
//            }
//        }
        valueSet = new RoaringBitmap();
        valueSet.deserialize(inPut);
    }
    
    private void readData()
    {
        if(data!=null && (valueSet == null || valueSet.isEmpty()))
        {
            ByteArrayInputStream stream = new ByteArrayInputStream(data);
            DataInputStream outputStream = new DataInputStream(stream);
            try
            {
                readData(outputStream);
                outputStream.close();
                data = null;
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
    public MeasureAggregator getCopy()
    {
        DistinctCountAggregator aggr = new DistinctCountAggregator(minValue);
        aggr.valueSet = valueSet.clone();//new HashSet<Double>(valueSet);
        return aggr;
    }
    
    @Override
    public int compareTo(MeasureAggregator measureAggr)
    {
        double compFixedVal = getValue();
        double otherVal = measureAggr.getValue();
        if(compFixedVal > otherVal)
        {
            return 1;
        }
        if(compFixedVal < otherVal) 
        {
            return -1; 
        }
        return 0;
    }

    @Override
    public MeasureAggregator get()
    {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(byteStream);
        try
        {
            writeData(outputStream);
        }
        catch(IOException ex)
        { 
//            ex.printStackTrace();
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, ex, ex.getMessage());
        }
        data = byteStream.toByteArray();
        valueSet = null;
        return this;
    }
    
    public String toString()
    {
        if(computedFixedValue == null)
        {
            readData();
            return valueSet.getCardinality()+"";
        }
        return computedFixedValue+"";
    }
    
    public RoaringBitmap getBitMap()
    {
        return valueSet;
    }
    
    public double getMinValue()
    {
        return minValue;
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
        double currentMinValue = buffer.getDouble();
        while(buffer.hasRemaining())
        {
            agg(buffer.getInt()+currentMinValue, null, 0, 0);
        }
    }

//    @Override
//    public void writeExternal(ObjectOutput out) throws IOException
//    {
//        valueSet.serialize(out);
//        
//    }
//
//    @Override
//    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
//    {
//        readData(in);
//    }
}
