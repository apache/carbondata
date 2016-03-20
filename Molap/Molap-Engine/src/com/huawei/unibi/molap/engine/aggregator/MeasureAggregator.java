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

package com.huawei.unibi.molap.engine.aggregator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 *
 * Class Description : MeasureAggregator interface. It will be implemented by
 * all the aggregator functions eg: sum, avg, max, min, etc, will be used for
 * aggregate the measure value based on kind of aggregator
 * 
 * Version 1.0
 */

public interface MeasureAggregator extends Serializable,Comparable<MeasureAggregator>
{
    /**
     * Aggregate function which will aggregate the new value with older value
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
    void agg(double newVal, byte[] key, int offset, int length);
    
    
    /**
     * Aggregate function which will aggregate the new value with older value
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
    void agg(Object newVal, byte[] key, int offset, int length);
//    
    byte[] getByteArray();
    
    /**
     * Overloaded Aggregate function will be used for Aggregate tables because
     * aggregate table will have fact_count as a measure.
     * 
     * @param newVal
     *            new value
     * @param factCount
     *            total fact count
     * 
     */
    void agg(double newVal, double factCount);

    /**
     * This method will return the aggregated value of the measure
     * 
     * @return aggregated value
     * 
     * 
     */
    double getValue();

    /**
     * This method return the object value of the MeasureAggregator
     * 
     * @return aggregated value
     * 
     */
    Object getValueObject();

    /**
     * This method merge the aggregated value based on aggregator passed
     * 
     * @param aggregator
     *            type of aggregator
     * 
     */
    void merge(MeasureAggregator aggregator);
    
    /**
     * This method will be used to set the new value
     * 
     * @param newValue
     *
     */
    void setNewValue(double newValue);
    
    /**
     * Is first time. It means it was never used for aggregating any value.
     * @return
     */
    boolean isFirstTime();
    
    /**
     * it creates the new copy of MeasureAggregator
     * @return MeasureAggregator
     */
    MeasureAggregator getCopy();
    
    /**
     * Write the state of the class to buffer
     * @param buffer
     */
    void writeData(DataOutput output) throws IOException;
    
    /**
     * Read the state of the class and set to the object
     * @param buffer
     */
    void readData(DataInput inPut) throws IOException;
    
    MeasureAggregator get();
    
    /**
     * Merge the byte arrays
     * 
     * @param value
     * 
     */
    void merge(byte[] value);
    
}
