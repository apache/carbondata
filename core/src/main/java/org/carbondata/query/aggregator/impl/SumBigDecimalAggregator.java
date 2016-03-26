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
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.carbondata.core.datastorage.store.dataholder.MolapReadDataHolder;
import org.carbondata.core.util.DataTypeUtil;
import org.carbondata.query.aggregator.MeasureAggregator;

/**
 * @author z00305190
 *
 */
public class SumBigDecimalAggregator extends AbstractMeasureAggregatorBasic {

    /**
     * serialVersionUID
     *
     */
    private static final long serialVersionUID = 623750056131364540L;

    /**
     * aggregate value
     */
    private BigDecimal aggVal;

    /**
     * This method will update the aggVal it will add new value to aggVal
     *
     * @param newVal
     *            new value
     *
     */
    @Override public void agg(Object newVal) {
        if (firstTime) {
            aggVal = (BigDecimal) newVal;
            firstTime = false;
        } else {
            aggVal = aggVal.add((BigDecimal) newVal);
        }
    }

    public void agg(MolapReadDataHolder newVal, int index) {
        BigDecimal valueBigDecimal = newVal.getReadableBigDecimalValueByIndex(index);
        aggVal = aggVal.add(valueBigDecimal);
        firstTime = false;
    }

    /**
     * Below method will be used to get the value byte array
     */
    @Override public byte[] getByteArray() {
        if (firstTime) {
            return new byte[0];
        }
        byte[] bytes = DataTypeUtil.bigDecimalToByte(aggVal);
        ByteBuffer allocate = ByteBuffer.allocate(4 + bytes.length);

        allocate.putInt(bytes.length);
        allocate.put(bytes);
        allocate.rewind();
        return allocate.array();
    }

    /**
     * This method will return aggVal
     *
     * @return sum value
     *
     */
    @Override public BigDecimal getBigDecimalValue() {
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
            agg(aggregator.getBigDecimalValue());
        }
    }

    /**
     * This method return the sum value as an object
     *
     * @return sum value as an object
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
        aggVal = (BigDecimal) newValue;
    }

    @Override public void readData(DataInput inPut) throws IOException {
        firstTime = inPut.readBoolean();
        aggVal = new BigDecimal(inPut.readUTF());
    }

    @Override public void writeData(DataOutput output) throws IOException {
        output.writeBoolean(firstTime);
        output.writeUTF(aggVal.toString());

    }

    @Override public MeasureAggregator getCopy() {
        SumBigDecimalAggregator aggr = new SumBigDecimalAggregator();
        aggr.aggVal = aggVal;
        aggr.firstTime = firstTime;
        return aggr;
    }

    @Override public void merge(byte[] value) {
        if (0 == value.length) {
            return;
        }

        ByteBuffer buffer = ByteBuffer.wrap(value);
        byte[] valueByte = new byte[buffer.getInt()];
        buffer.get(valueByte);
        //        BigDecimal valueBigDecimal = new BigDecimal(new String(valueByte));
        BigDecimal valueBigDecimal = DataTypeUtil.byteToBigDecimal(valueByte);
        aggVal = aggVal.add(valueBigDecimal);
        firstTime = false;
    }

    public String toString() {
        return aggVal + "";
    }

    @Override public int compareTo(MeasureAggregator o) {
        BigDecimal value = getBigDecimalValue();
        BigDecimal otherVal = o.getBigDecimalValue();

        return value.compareTo(otherVal);
    }
}
