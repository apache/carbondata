/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdEVzw1icjfRowqz2DW4XzUpEhhSzBOwVynEHjc
u0090fUfdjWEkccKe4Y/GlbooVMj1KehmvZGK5hDgAnVApUjAvQtlp8Lu0WseUP5anZyDBDh
6kvDDAW/B/sKJeVCKc4vFsWCQAeKJIt78i2Q9FVTaS9c1SNsIZAmKxLVnR7+OQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
 */
package com.huawei.unibi.molap.engine.aggregator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Project Name NSE V3R7C00
 * 
 * Module Name : Molap Engine
 * 
 * Author K00900841
 * 
 * Created Date :13-May-2013 3:35:33 PM
 * 
 * FileName : MeasureAggregator.java
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
