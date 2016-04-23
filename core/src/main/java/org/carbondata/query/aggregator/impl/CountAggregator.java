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

import org.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.query.aggregator.MeasureAggregator;

/**
 * Class Description : It will return total count of values
 */
public class CountAggregator implements MeasureAggregator {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 2678878935295306313L;

    /**
     * aggregate value
     */
    private double aggVal;

    /**
     * Count Aggregate function which update the total count
     *
     * @param newVal new value
     */
    @Override
    public void agg(double newVal) {
        aggVal++;
    }

    /**
     * Count Aggregate function which update the total count
     *
     * @param newVal new value
     */
    @Override
    public void agg(Object newVal) {
        aggVal++;
    }

    @Override
    public void agg(MeasureColumnDataChunk dataChunk, int index) {
    	if(!dataChunk.getNullValueIndexHolder().getBitSet().get(index))
    	{
    		aggVal++;
    	}
    }

    /**
     * Below method will be used to get the value byte array
     */
    @Override
    public byte[] getByteArray() {
        ByteBuffer buffer = ByteBuffer.allocate(CarbonCommonConstants.DOUBLE_SIZE_IN_BYTE);
        buffer.putDouble(aggVal);
        return buffer.array();
    }

    /**
     * Returns the total count
     *
     * @return total count
     */
    @Override
    public Double getDoubleValue() {
        return aggVal;
    }

    @Override
    public Long getLongValue() {
        return (long) aggVal;
    }

    @Override
    public BigDecimal getBigDecimalValue() {
        return new BigDecimal(aggVal);
    }

    /**
     * Merge the total count with the aggregator
     *
     * @param aggregator count aggregator
     */
    @Override
    public void merge(MeasureAggregator aggregator) {
        CountAggregator countAggregator = (CountAggregator) aggregator;
        aggVal += countAggregator.aggVal;
    }

    /**
     * Overloaded Aggregate function will be used for Aggregate tables because
     * aggregate table will have fact_count as a measure. It will update the
     * total count
     *
     * @param newVal
     *            new value
     * @param factCount
     *            total fact count
     *
     */
    //    @Override
    //    public void agg(double newVal, double factCount)
    //    {
    //        agg(newVal, null, 0, 0);
    //    }

    /**
     * This method return the count value as an object
     *
     * @return count value as an object
     */

    @Override
    public Object getValueObject() {
        return aggVal;
    }

    /**
     * @see MeasureAggregator#setNewValue(Object)
     */
    @Override
    public void setNewValue(Object newValue) {
        aggVal += Double.parseDouble(String.valueOf(newValue));
    }

    @Override
    public boolean isFirstTime() {
        return false;
    }

    @Override
    public void writeData(DataOutput output) throws IOException {
        output.writeDouble(aggVal);

    }

    @Override
    public void readData(DataInput inPut) throws IOException {
        aggVal = inPut.readDouble();
    }

    @Override
    public MeasureAggregator getCopy() {
        CountAggregator aggregator = new CountAggregator();
        aggregator.aggVal = aggVal;
        return aggregator;
    }

    //we are not comparing the Aggregator values 
   /* public boolean equals(MeasureAggregator msrAggregator){
        return compareTo(msrAggregator)==0;
    }*/

    @Override
    public void merge(byte[] value) {
        if (0 == value.length) {
            return;
        }
        ByteBuffer buffer = ByteBuffer.wrap(value);
        aggVal += buffer.getDouble();
    }

    @Override
    public int compareTo(MeasureAggregator obj) {
        double val = getDoubleValue();
        double otherVal = obj.getDoubleValue();
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
        return this;
    }

    public String toString() {
        return aggVal + "";
    }

	@Override
	public MeasureAggregator getNew() {
		return new CountAggregator();
	}

}
