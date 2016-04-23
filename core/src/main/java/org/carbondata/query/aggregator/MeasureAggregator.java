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

package org.carbondata.query.aggregator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;

import org.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;

/**
 * Class Description : MeasureAggregator interface. It will be implemented by
 * all the aggregator functions eg: sum, avg, max, min, etc, will be used for
 * aggregate the measure value based on kind of aggregator
 */

public interface MeasureAggregator extends Serializable, Comparable<MeasureAggregator> {

    /**
     * Below method will be used to aggregate the Double value
     *
     * @param newVal
     */
    void agg(double newVal);

    /**
     * Below method will be used to aggregate the object value
     *
     * @param newVal
     */
    void agg(Object newVal);

    /**
     * Below method will be used to aggregate the value based on index
     *
     * @param newVal
     * @param index
     */
    void agg(MeasureColumnDataChunk newVal, int index);

    /**
     * Get the Serialize byte array
     *
     * @return
     */
    byte[] getByteArray();

    /**
     * This method will be used to set the new value
     *
     * @param newValue
     */
    void setNewValue(Object newValue);

    /**
     * This method return the object value of the MeasureAggregator
     *
     * @return aggregated value
     */
    Object getValueObject();

    /**
     * This method return the object value of the MeasureAggregator
     *
     * @return aggregated value
     */
    Double getDoubleValue();

    /**
     * This method return the object value of the MeasureAggregator
     *
     * @return aggregated value
     */
    Long getLongValue();

    BigDecimal getBigDecimalValue();

    /**
     * This method merge the aggregated value based on aggregator passed
     *
     * @param aggregator type of aggregator
     */
    void merge(MeasureAggregator aggregator);

    /**
     * Is first time. It means it was never used for aggregating any value.
     *
     * @return
     */
    boolean isFirstTime();

    /**
     * it creates the new copy of MeasureAggregator
     *
     * @return MeasureAggregator
     */
    MeasureAggregator getCopy();

    /**
     * Write the state of the class to buffer
     *
     */
    void writeData(DataOutput output) throws IOException;

    /**
     * Read the state of the class and set to the object
     *
     */
    void readData(DataInput inPut) throws IOException;

    MeasureAggregator get();

    /**
     * Merge the byte arrays
     *
     * @param value
     */
    void merge(byte[] value);
    
    /**
     * Below method will be used to get the 
     * new instance
     * @return new instance
     */
    MeasureAggregator getNew();
}
