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

package org.carbondata.query.aggregator.impl;

import java.io.*;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.datastorage.store.dataholder.MolapReadDataHolder;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.util.MolapEngineLogEvent;
import org.roaringbitmap.RoaringBitmap;

/**
 * @author K00900841
 *         The distinct count aggregator
 *         Ex:
 *         ID NAME Sales
 *         <p>1 a 200
 *         <p>2 a 100
 *         <p>3 a 200
 *         select count(distinct sales) # would result 2
 *         select count(sales) # would result 3
 */
public class SurrogateBasedDistinctCountAggregator implements MeasureAggregator {
    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 6313463368629960186L;

    private static final LogService LOGGER =
            LogServiceFactory.getLogService(SurrogateBasedDistinctCountAggregator.class.getName());
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
    public SurrogateBasedDistinctCountAggregator() {
        bitSet = new RoaringBitmap();
    }

    /**
     * just need to add the unique values to agg set
     */
    @Override
    public void agg(double newVal) {//CHECKSTYLE:OFF    Approval No:Approval-V3R8C00_013
        int a = (int) newVal;//CHECKSTYLE:ON
        bitSet.add(a);
    }

    /**
     * Distinct count Aggregate function which update the Distinct count
     *
     * @param newVal new value
     */
    @Override
    public void agg(Object newVal) {
        // Object include double
        if (newVal instanceof Double) {
            agg((double) newVal);
            return;
        }

        byte[] values = (byte[]) newVal;
        ByteBuffer buffer = ByteBuffer.wrap(values);
        buffer.rewind();
        //CHECKSTYLE:OFF    Approval No:Approval-V3R8C00_018
        while (buffer
                .hasRemaining()) { //CHECKSTYLE:ON  //CHECKSTYLE:OFF    Approval No:Approval-V3R8C00_013
            bitSet.add((int) buffer.getDouble());//CHECKSTYLE:ON
        }
    }

    public void agg(MolapReadDataHolder newVal, int index) {
        int a = (int) newVal.getReadableDoubleValueByIndex(index);//CHECKSTYLE:ON
        bitSet.add(a);
    }

    /**
     * Below method will be used to get the value byte array
     */
    @Override
    public byte[] getByteArray() {
        return null;
    }

    //    @Override
    //    public void agg(double newVal, double factCount)
    //    {
    //
    //    }

    private void agg(RoaringBitmap bitSet2) {
        bitSet.or(bitSet2);
    }

    /**
     * merge the valueset so that we get the count of unique values
     */
    @Override
    public void merge(MeasureAggregator aggregator) {
        SurrogateBasedDistinctCountAggregator distinctCountAggregator =
                (SurrogateBasedDistinctCountAggregator) aggregator;
        readData();
        distinctCountAggregator.readData();
        agg(distinctCountAggregator.bitSet);
    }

    @Override
    public Double getDoubleValue() {
        if (computedFixedValue == null) {
            readData();
            return (double) bitSet.getCardinality();
        }
        return computedFixedValue;
    }

    @Override
    public Long getLongValue() {
        if (computedFixedValue == null) {
            readData();
            return (long) bitSet.getCardinality();
        }
        return computedFixedValue.longValue();
    }

    @Override
    public BigDecimal getBigDecimalValue() {
        if (computedFixedValue == null) {
            readData();
            return new BigDecimal(bitSet.getCardinality());
        }
        return new BigDecimal(computedFixedValue);
    }

    @Override
    public Object getValueObject() {
        return bitSet.getCardinality();
    }

    /**
     * @see MeasureAggregator#setNewValue(Object)
     */
    @Override
    public void setNewValue(Object newValue) {
        computedFixedValue = (Double) newValue;
        bitSet = null;
    }

    @Override
    public boolean isFirstTime() {
        return false;
    }

    @Override
    public void writeData(DataOutput output) throws IOException {
        bitSet.serialize(output);
    }

    @Override
    public void readData(DataInput inPut) throws IOException {
        bitSet = new RoaringBitmap();
        bitSet.deserialize(inPut);
    }

    @Override
    public MeasureAggregator getCopy() {
        SurrogateBasedDistinctCountAggregator aggregator =
                new SurrogateBasedDistinctCountAggregator();
        aggregator.bitSet = bitSet.clone();
        return aggregator;
    }

    //we are not comparing the Aggregator values 
    /* public boolean equals(MeasureAggregator msrAggregator){
         return compareTo(msrAggregator)==0;
     }*/

    @Override
    public int compareTo(MeasureAggregator object) {
        double val = getDoubleValue();
        double otherVal = object.getDoubleValue();
        if (val > otherVal) {
            return 1;
        }
        if (val < otherVal) {
            return -1;
        }
        return 0;
    }

    @Override
    public MeasureAggregator get() {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(stream);
        try {
            writeData(outputStream);
        } catch (IOException e) {
            //            e.printStackTrace();
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, e.getMessage());
        }
        data = stream.toByteArray();
        bitSet = null;
        return this;
    }

    private void readData() {
        if (bitSet == null || bitSet.isEmpty()) {
            ByteArrayInputStream stream = new ByteArrayInputStream(data);
            DataInputStream outputStream = new DataInputStream(stream);
            try {
                readData(outputStream);
                outputStream.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                //                e.printStackTrace();
                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, e.getMessage());
            }
        }
    }

    @Override
    public void merge(byte[] value) {
        // TODO Auto-generated method stub

    }
}