/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdEVzw1icjfRowqz2DW4XzUpEhhSzBOwVynEHjc
u0090YeyNJjyiBxlZZhvq198q+Px/O6umGvGwr5h9OKhpMctsfEvwH0Ku71ImcKU6VAJ7mHZ
e2xQU1gqw8DAe8i5OCRnjPMmOC9dX8zPk/kKPGifGLgFauScMSF4Lt2p+I7MLQ==*/
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

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.carbondata.query.aggregator.MeasureAggregator;

/**
 * Project Name NSE V3R7C00
 * Module Name : Molap Engine
 * Author K00900841
 * Created Date :13-May-2013 3:35:33 PM
 * FileName : AvgAggregator.java
 * Class Description :
 * It will return average of aggregate values
 * Version 1.0
 */

public class AvgLongAggregator extends AbstractMeasureAggregatorBasic {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 5463736686281089871L;

    /**
     * total number of aggregate values
     */
    protected double count;

    /**
     * aggregate value
     */
    protected long aggVal;

    /**
     * Average Aggregate function which will add all the aggregate values and it
     * will increment the total count every time, for average value
     *
     * @param newVal new value
     */
    @Override
    public void agg(Object newVal) {
        if (newVal instanceof byte[]) {
            ByteBuffer buffer = ByteBuffer.wrap((byte[]) newVal);
            buffer.rewind();
            //CHECKSTYLE:OFF    Approval No:Approval-V3R8C00_018
            while (buffer.hasRemaining()) { //CHECKSTYLE:ON
                aggVal += buffer.getLong();
                count += buffer.getDouble();
                firstTime = false;
            }
            return;
        }
        aggVal += (Long) newVal;
        count++;
        firstTime = false;
    }

    @Override
    public void agg(CarbonReadDataHolder newVal, int index) {
        byte[] value = newVal.getReadableByteArrayValueByIndex(index);
        ByteBuffer buffer = ByteBuffer.wrap(value);
        aggVal += buffer.getLong();
        count += buffer.getDouble();
        firstTime = false;
    }

    /**
     * Below method will be used to get the value byte array
     */
    @Override
    public byte[] getByteArray() {
        if (firstTime) {
            return new byte[0];
        }
        ByteBuffer buffer = ByteBuffer.allocate(
                CarbonCommonConstants.LONG_SIZE_IN_BYTE + CarbonCommonConstants.DOUBLE_SIZE_IN_BYTE);
        buffer.putLong(aggVal);
        buffer.putDouble(count);
        return buffer.array();
    }

    @Override
    public Long getLongValue() {
        return aggVal / (long) count;
    }

    /**
     * This method merge the aggregated value, in average aggregator it will add
     * count and aggregate value
     *
     * @param aggregator Avg Aggregator
     */
    @Override
    public void merge(MeasureAggregator aggregator) {
        AvgLongAggregator avgAggregator = (AvgLongAggregator) aggregator;
        if (!avgAggregator.isFirstTime()) {
            aggVal += avgAggregator.aggVal;
            count += avgAggregator.count;
            firstTime = false;
        }
    }

    /**
     * This method return the average value as an object
     *
     * @return average value as an object
     */
    @Override
    public Object getValueObject() {
        return aggVal / count;
    }

    /**
     * @see MeasureAggregator#setNewValue(Object)
     */
    @Override
    public void setNewValue(Object newValue) {
        aggVal = (Long) newValue;
        count = 1;
    }

    @Override
    public void writeData(DataOutput output) throws IOException {
        output.writeBoolean(firstTime);
        output.writeLong(aggVal);
        output.writeDouble(count);

    }

    @Override
    public void readData(DataInput inPut) throws IOException {
        firstTime = inPut.readBoolean();
        aggVal = inPut.readLong();
        count = inPut.readDouble();
    }

    @Override
    public MeasureAggregator getCopy() {
        AvgLongAggregator avg = new AvgLongAggregator();
        avg.aggVal = aggVal;
        avg.count = count;
        avg.firstTime = firstTime;
        return avg;
    }

    //we are not comparing any Aggregator values
    /*public boolean equals(MeasureAggregator msrAggregator){
        return compareTo(msrAggregator)==0;
    }*/

    @Override
    public int compareTo(MeasureAggregator o) {
        long val = getLongValue();
        long otherVal = o.getLongValue();
        if (val > otherVal) {
            return 1;
        } else if (val < otherVal) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public void merge(byte[] value) {
        if (0 == value.length) {
            return;
        }
        ByteBuffer buffer = ByteBuffer.wrap(value);
        aggVal += buffer.getLong();
        count += buffer.getDouble();
        firstTime = false;
    }

    public String toString() {
        return (aggVal / count) + "";
    }
}
