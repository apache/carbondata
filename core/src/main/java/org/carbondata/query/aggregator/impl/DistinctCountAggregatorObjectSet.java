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
import java.util.Set;

import org.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.carbondata.query.aggregator.MeasureAggregator;

public class DistinctCountAggregatorObjectSet implements MeasureAggregator {

    private static final long serialVersionUID = 6313463368629960186L;

    private Set<Object> valueSetForObj;

    public DistinctCountAggregatorObjectSet() {
        valueSetForObj = new HashSet<Object>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    }

    /**
     * just need to add the unique values to agg set
     */
    @Override
    public void agg(double newVal) {
        valueSetForObj.add(newVal);
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
        while (buffer.hasRemaining()) {
            valueSetForObj.add(buffer.getDouble());
        }
    }

    @Override
    public void agg(MeasureColumnDataChunk dataChunk, int index) {
    	if(!dataChunk.getNullValueIndexHolder().getBitSet().get(index))
    	{
    		valueSetForObj.add(dataChunk.getMeasureDataHolder().getReadableDoubleValueByIndex(index));
    	}
    }

    /**
     * Below method will be used to get the value byte array
     */
    @Override
    public byte[] getByteArray() {
        return null;
    }

    private void agg(Set<Object> set2) {
        valueSetForObj.addAll(set2);
    }

    /**
     * merge the valueset so that we get the count of unique values
     */
    @Override
    public void merge(MeasureAggregator aggregator) {
        DistinctCountAggregatorObjectSet distinctCountAggregator =
                (DistinctCountAggregatorObjectSet) aggregator;
        agg(distinctCountAggregator.valueSetForObj);
    }

    @Override
    public Double getDoubleValue() {
        return (double) valueSetForObj.size();
    }

    @Override
    public Long getLongValue() {
        return (long) valueSetForObj.size();
    }

    @Override
    public BigDecimal getBigDecimalValue() {
        return new BigDecimal(valueSetForObj.size());
    }

    @Override
    public Object getValueObject() {
        return valueSetForObj.size();
    }

    @Override
    public void setNewValue(Object newValue) {
        valueSetForObj.add(newValue);
    }

    @Override
    public boolean isFirstTime() {
        return false;
    }

    @Override
    public void writeData(DataOutput output) throws IOException {

    }

    @Override
    public void readData(DataInput inPut) throws IOException {

    }

    @Override
    public MeasureAggregator getCopy() {

        DistinctCountAggregatorObjectSet aggregator = new DistinctCountAggregatorObjectSet();
        aggregator.valueSetForObj = new HashSet<Object>(valueSetForObj);
        return aggregator;
    }

    @Override
    public int compareTo(MeasureAggregator measureAggr) {
        double valueSetForObjSize = getDoubleValue();
        double otherVal = measureAggr.getDoubleValue();
        if (valueSetForObjSize > otherVal) {
            return 1;
        }
        if (valueSetForObjSize < otherVal) {
            return -1;
        }
        return 0;
    }

    @Override
    public MeasureAggregator get() {
        return this;
    }

    public String toString() {
        return valueSetForObj.size() + "";
    }

    @Override
    public void merge(byte[] value) {
    }

	@Override
	public MeasureAggregator getNew() {
		return new DistinctCountAggregatorObjectSet();
	}

}
