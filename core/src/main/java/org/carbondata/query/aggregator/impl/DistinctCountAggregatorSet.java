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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.datastorage.store.dataholder.MolapReadDataHolder;
import org.carbondata.query.aggregator.MeasureAggregator;

/**
 * * The distinct count aggregator
 * Ex:
 * ID NAME Sales
 * <p>1 a 200
 * <p>2 a 100
 * <p>3 a 200
 * select count(distinct sales) # would result 2
 * select count(sales) # would result 3
 */
public class DistinctCountAggregatorSet implements MeasureAggregator {
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
    private Set<Double> valueSet;

    public DistinctCountAggregatorSet() {
        valueSet = new HashSet<Double>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    }

    /**
     * just need to add the unique values to agg set
     */
    @Override
    public void agg(double newVal) {
        valueSet.add(newVal);
    }

    /**
     * Below method will be used to get the value byte array
     */
    @Override
    public byte[] getByteArray() {
        Iterator<Double> iterator = valueSet.iterator();
        ByteBuffer buffer =
                ByteBuffer.allocate(valueSet.size() * MolapCommonConstants.DOUBLE_SIZE_IN_BYTE);
        //CHECKSTYLE:OFF    Approval No:Approval-V3R8C00_018
        while (iterator.hasNext()) { //CHECKSTYLE:ON
            buffer.putDouble(iterator.next());
        }
        buffer.rewind();
        return buffer.array();
    }

    //TODO SIMIAN

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
        while (buffer.hasRemaining()) { //CHECKSTYLE:ON
            valueSet.add(buffer.getDouble());
        }
    }

    @Override
    public void agg(MolapReadDataHolder newVal, int index) {
        valueSet.add(newVal.getReadableDoubleValueByIndex(index));
    }

    //    @Override
    //    public void agg(double newVal, double factCount)
    //    {
    //
    //    }

    private void agg(Set<Double> set2) {
        valueSet.addAll(set2);
    }

    /**
     * merge the valueset so that we get the count of unique values
     */
    @Override
    public void merge(MeasureAggregator aggregator) {
        DistinctCountAggregatorSet distinctCountAggregator =
                (DistinctCountAggregatorSet) aggregator;
        agg(distinctCountAggregator.valueSet);
    }

    @Override
    public Double getDoubleValue() {
        if (computedFixedValue == null) {
            return (double) valueSet.size();
        }
        return computedFixedValue;
    }

    @Override
    public Long getLongValue() {
        if (computedFixedValue == null) {
            return (long) valueSet.size();
        }
        return computedFixedValue.longValue();
    }

    @Override
    public BigDecimal getBigDecimalValue() {
        if (computedFixedValue == null) {
            return new BigDecimal(valueSet.size());
        }
        return new BigDecimal(computedFixedValue);
    }

    @Override
    public Object getValueObject() {
        return valueSet.size();
    }

    @Override
    public boolean isFirstTime() {
        return false;
    }

    /**
     * @see MeasureAggregator#setNewValue(Object)
     */
    @Override
    public void setNewValue(Object newValue) {
        computedFixedValue = (Double) newValue;
        valueSet = null;
    }

    @Override
    public void writeData(DataOutput dataOutputVal) throws IOException {

        if (computedFixedValue != null) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 8);
            byteBuffer.putInt(-1);
            byteBuffer.putDouble(computedFixedValue);
            byteBuffer.flip();
            dataOutputVal.write(byteBuffer.array());
        } else {
            int length = valueSet.size() * 8;
            ByteBuffer byteBuffer = ByteBuffer.allocate(length + 4 + 1);
            byteBuffer.putInt(length);
            for (double val : valueSet) {
                byteBuffer.putDouble(val);
            }
            byteBuffer.flip();
            dataOutputVal.write(byteBuffer.array());
        }
    }

    @Override
    public void readData(DataInput inPutVal) throws IOException {
        int length = inPutVal.readInt();

        if (length == -1) {
            computedFixedValue = inPutVal.readDouble();
            valueSet = null;
        } else {
            length = length / 8;
            valueSet = new HashSet<Double>(length + 1, 1.0f);
            for (int i = 0; i < length; i++) {
                valueSet.add(inPutVal.readDouble());
            }
        }

    }

    @Override
    public MeasureAggregator getCopy() {

        DistinctCountAggregatorSet aggregator = new DistinctCountAggregatorSet();
        aggregator.valueSet = new HashSet<Double>(valueSet);
        return aggregator;
    }

    //we are not comparing the Aggregator values 
    /* public boolean equals(MeasureAggregator msrAggregator){
         return compareTo(msrAggregator)==0;
     }*/

    public String toString() {
        if (computedFixedValue == null) {
            return valueSet.size() + "";
        }
        return computedFixedValue + "";
    }

    @Override
    public int compareTo(MeasureAggregator msr) {
        double msrVal = getDoubleValue();
        double otherMsrVal = msr.getDoubleValue();
        if (msrVal > otherMsrVal) {
            return 1;
        }
        if (msrVal < otherMsrVal) {
            return -1;
        }
        return 0;
    }

    @Override
    public MeasureAggregator get() {
        return this;
    }

    @Override
    public void merge(byte[] value) {
        // TODO Auto-generated method stub

    }

}
