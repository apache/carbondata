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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.util.CarbonEngineLogEvent;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

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
public class DistinctCountAggregator implements MeasureAggregator {

    private static final LogService LOGGER =
            LogServiceFactory.getLogService(DistinctCountAggregator.class.getName());
    /**
     *
     */
    private static final long serialVersionUID = 6313463368629960186L;
    /**
     * For Spark CARBON to avoid heavy object transfer it better to flatten the Aggregators. There is no aggregation expected after setting this value.
     */
    private Double computedFixedValue;
    /**
     *
     */
    //    private Set<Double> valueSet;
    private transient RoaringBitmap valueSet;

    private byte[] data;

    private double minValue;

    public DistinctCountAggregator(Object minValue) {
        valueSet = new RoaringBitmap();
        if (minValue instanceof BigDecimal) {
            this.minValue = ((BigDecimal) minValue).doubleValue();
        } else if (minValue instanceof Long) {
            this.minValue = ((Long) minValue).doubleValue();
        } else {
            this.minValue = (Double) minValue;
        }
    }

    public DistinctCountAggregator() {
        valueSet = new RoaringBitmap();
    }

    /**
     * just need to add the unique values to agg set
     */
    @Override
    public void agg(double newVal) {
        valueSet.add((int) (newVal - minValue));
    }

    /**
     * Distinct count Aggregate function which update the Distinct count
     *
     * @param newVal new value
     */
    @Override
    public void agg(Object newVal) {
        if (newVal instanceof byte[]) {
            byte[] values = (byte[]) newVal;
            ByteBuffer buffer = ByteBuffer.wrap(values);
            buffer.rewind();
            while (buffer.hasRemaining()) {
                valueSet.add(buffer.getInt());
            }
            return;
        } else {
            double value = new Double(newVal.toString());
            agg(value);
        }
    }

    @Override
    public void agg(MeasureColumnDataChunk dataChunk, int index) {
    	if(!dataChunk.getNullValueIndexHolder().getBitSet().get(index))
    	{
    		valueSet.add((int)dataChunk.getMeasureDataHolder().getReadableDoubleValueByIndex(index));
    	}
    }

    /**
     * Below method will be used to get the value byte array
     */
    @Override
    public byte[] getByteArray() {
        if (valueSet.getCardinality() == 0) {
            return new byte[0];
        }
        IntIterator iterator = valueSet.getIntIterator();
        ByteBuffer buffer = ByteBuffer.allocate(valueSet.getCardinality() * 4 + 8);
        buffer.putDouble(minValue);
        while (iterator.hasNext()) {
            buffer.putInt(iterator.next());
        }
        buffer.rewind();
        return buffer.array();
    }

    private void agg(RoaringBitmap set2, double minValue) {
        if (this.minValue == minValue) {
            valueSet.or(set2);
        } else {
            if (this.minValue > minValue) {
                IntIterator intIterator = valueSet.getIntIterator();
                while (intIterator.hasNext()) {
                    set2.add((int) ((double) (intIterator.next() + this.minValue) - minValue));
                }
                this.minValue = minValue;
                this.valueSet = set2;
            } else {
                IntIterator intIterator = set2.getIntIterator();
                while (intIterator.hasNext()) {
                    valueSet.add((int) ((double) (intIterator.next() + minValue) - this.minValue));
                }
            }
        }
    }

    /**
     * merge the valueset so that we get the count of unique values
     */
    @Override
    public void merge(MeasureAggregator aggregator) {
        DistinctCountAggregator distinctCountAggregator = (DistinctCountAggregator) aggregator;
        readData();
        distinctCountAggregator.readData();
        if (distinctCountAggregator.valueSet != null) {
            agg(distinctCountAggregator.valueSet, distinctCountAggregator.minValue);
        }
    }

    @Override
    public Double getDoubleValue() {
        if (computedFixedValue == null) {
            readData();
            return (double) valueSet.getCardinality();
        }
        return computedFixedValue;
    }

    @Override
    public Long getLongValue() {
        if (computedFixedValue == null) {
            readData();
            return (long) valueSet.getCardinality();
        }
        return computedFixedValue.longValue();
    }

    @Override
    public BigDecimal getBigDecimalValue() {
        if (computedFixedValue == null) {
            readData();
            return new BigDecimal(valueSet.getCardinality());
        }
        return new BigDecimal(computedFixedValue);
    }

    @Override
    public Object getValueObject() {
        return valueSet.getCardinality();
    }

    @Override
    public void setNewValue(Object newValue) {
        computedFixedValue = (Double) newValue;
        valueSet = null;
    }

    @Override
    public boolean isFirstTime() {
        return false;
    }

    @Override
    public void writeData(DataOutput output) throws IOException {

        if (computedFixedValue != null) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 8);
            byteBuffer.putInt(-1);
            byteBuffer.putDouble(computedFixedValue);
            byteBuffer.flip();
            output.write(byteBuffer.array());
        } else {
            if (valueSet != null) {
                valueSet.serialize(output);
            } else {
                output.write(data);
            }
        }
    }

    @Override
    public void readData(DataInput inPut) throws IOException {
        valueSet = new RoaringBitmap();
        valueSet.deserialize(inPut);
    }

    private void readData() {
        if (data != null && (valueSet == null || valueSet.isEmpty())) {
            ByteArrayInputStream stream = new ByteArrayInputStream(data);
            DataInputStream outputStream = new DataInputStream(stream);
            try {
                readData(outputStream);
                outputStream.close();
                data = null;
            } catch (IOException e) {
                LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e, e.getMessage());
            }
        }
    }

    @Override
    public MeasureAggregator getCopy() {
        DistinctCountAggregator aggr = new DistinctCountAggregator(minValue);
        aggr.valueSet = valueSet.clone();
        return aggr;
    }

    @Override
    public int compareTo(MeasureAggregator measureAggr) {
        double compFixedVal = getDoubleValue();
        double otherVal = measureAggr.getDoubleValue();
        if (compFixedVal > otherVal) {
            return 1;
        }
        if (compFixedVal < otherVal) {
            return -1;
        }
        return 0;
    }

    @Override
    public MeasureAggregator get() {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(byteStream);
        try {
            writeData(outputStream);
        } catch (IOException ex) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, ex, ex.getMessage());
        }
        data = byteStream.toByteArray();
        valueSet = null;
        return this;
    }

    public String toString() {
        if (computedFixedValue == null) {
            readData();
            return valueSet.getCardinality() + "";
        }
        return computedFixedValue + "";
    }

    public RoaringBitmap getBitMap() {
        return valueSet;
    }

    public double getMinValue() {
        return minValue;
    }

    @Override
    public void merge(byte[] value) {
        if (0 == value.length) {
            return;
        }
        ByteBuffer buffer = ByteBuffer.wrap(value);
        buffer.rewind();
        double currentMinValue = buffer.getDouble();
        while (buffer.hasRemaining()) {
            agg(buffer.getInt() + currentMinValue);
        }
    }

	@Override
	public MeasureAggregator getNew() {
		// TODO Auto-generated method stub
		return new DistinctCountAggregator();
	}

}
