/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdEVzw1icjfRowqz2DW4XzUpEhhSzBOwVynEHjc
u0090fbnj+0VbOfZnjQdUjNGeZBp/OEV/ihcZz/8Bj30H6cdHtnLWokryD8YEIDSBoqj0HMv
x2bWOm2rwPhsF8R5ByGyW4CmQm6QiI4mdcr/+CnCQ2iadvOiXEuMjfxMA+hszQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 */
package org.carbondata.query.aggregator.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.datastorage.store.dataholder.MolapReadDataHolder;
import org.carbondata.query.aggregator.MeasureAggregator;

/**
 * @author z00305190
 *
 */

public class SumLongAggregator extends AbstractMeasureAggregatorBasic {

    /**
     * serialVersionUID
     *
     */
    private static final long serialVersionUID = 623750056131364540L;

    /**
     * aggregate value
     */
    private long aggVal;

    /**
     * This method will update the aggVal it will add new value to aggVal
     *
     * @param newVal
     *            new value
     *
     */
    @Override public void agg(Object newVal) {
        aggVal += (long) newVal;
        firstTime = false;
    }

    public void agg(MolapReadDataHolder newVal, int index) {
        aggVal += newVal.getReadableLongValueByIndex(index);
        firstTime = false;
    }

    /**
     * Below method will be used to get the value byte array
     */
    @Override public byte[] getByteArray() {
        if (firstTime) {
            return new byte[0];
        }
        ByteBuffer buffer = ByteBuffer.allocate(MolapCommonConstants.LONG_SIZE_IN_BYTE);
        buffer.putLong(aggVal);
        return buffer.array();
    }

    /**
     * This method will return aggVal
     *
     * @return sum value
     *
     */
    @Override public Long getLongValue() {
        return aggVal;
    }

    /* Merge the value, it will update the sum aggregate value it will add new
     * value to aggVal
     * 
     * @param aggregator
     *            SumAggregator
     * 
     */
    @Override public void merge(MeasureAggregator aggregator) {
        if (!aggregator.isFirstTime()) {
            agg(aggregator.getLongValue());
        }
    }

    /**
     * This method return the sum value as an object
     *
     * @return sum long value as an object
     */
    @Override public Object getValueObject() {
        return aggVal;
    }

    /**
     *
     * @see MeasureAggregator#setNewValue(Object)
     *
     */
    @Override public void setNewValue(Object newValue) {
        aggVal = (long) newValue;
    }

    //TODO SIMIAN
    @Override public void readData(DataInput inPut) throws IOException {
        firstTime = inPut.readBoolean();
        aggVal = inPut.readLong();
    }

    @Override public void writeData(DataOutput output) throws IOException {
        output.writeBoolean(firstTime);
        output.writeLong(aggVal);

    }

    @Override public MeasureAggregator getCopy() {
        SumLongAggregator aggr = new SumLongAggregator();
        aggr.aggVal = aggVal;
        aggr.firstTime = firstTime;
        return aggr;
    }

    @Override public void merge(byte[] value) {
        if (0 == value.length) {
            return;
        }
        aggVal += ByteBuffer.wrap(value).getLong();
        firstTime = false;
    }

    public String toString() {
        return aggVal + "";
    }

    @Override public int compareTo(MeasureAggregator o) {
        Long value = getLongValue();
        Long otherVal = o.getLongValue();
        if (value > otherVal) {
            return 1;
        }
        if (value < otherVal) {
            return -1;
        }
        return 0;
    }
}
