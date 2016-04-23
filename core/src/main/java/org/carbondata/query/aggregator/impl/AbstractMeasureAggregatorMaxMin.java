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

import java.math.BigDecimal;

import org.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.carbondata.query.aggregator.MeasureAggregator;

/**
 * AbstractMeasureAggregatorMaxMin
 * Used for custom Carbon Aggregator max  min
 */
public abstract class AbstractMeasureAggregatorMaxMin implements MeasureAggregator {
    private static final long serialVersionUID = 1L;

    protected Comparable<Object> aggVal;

    protected boolean firstTime = true;

    protected abstract void internalAgg(Object value);

    @Override
    public void agg(double newVal) {
        internalAgg((Double) newVal);
        firstTime = false;
    }

    @Override
    public void agg(Object newVal) {
        internalAgg(newVal);
        firstTime = false;
    }

    @Override
    public void agg(MeasureColumnDataChunk dataChunk, int index) {
    	if(!dataChunk.getNullValueIndexHolder().getBitSet().get(index))
    	{
    		internalAgg(dataChunk.getMeasureDataHolder().getReadableDoubleValueByIndex(index));
    		firstTime = false;
    	}
    }

    @Override
    public Double getDoubleValue() {
        return (Double) ((Object) aggVal);
    }

    @Override
    public Long getLongValue() {
        return (Long) ((Object) aggVal);
    }

    @Override
    public BigDecimal getBigDecimalValue() {
        return (BigDecimal) ((Object) aggVal);
    }

    /**
     * @see MeasureAggregator#setNewValue(Object)
     */
    @Override
    public void setNewValue(Object newValue) {
    }

    /**
     * This method return the max value as an object
     *
     * @return max value as an object
     */
    @Override
    public Object getValueObject() {
        return aggVal;
    }

    @Override
    public boolean isFirstTime() {
        return firstTime;
    }

    @Override
    public MeasureAggregator get() {
        return this;

    }

    public String toString() {
        return aggVal + "";
    }

    @Override
    public int compareTo(MeasureAggregator msrAggr) {
        @SuppressWarnings("unchecked")
        Comparable<Object> other = (Comparable<Object>) msrAggr.getValueObject();

        return aggVal.compareTo(other);
    }
}
