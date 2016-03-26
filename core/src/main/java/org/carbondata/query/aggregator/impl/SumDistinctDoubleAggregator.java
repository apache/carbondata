package org.carbondata.query.aggregator.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.datastorage.store.dataholder.MolapReadDataHolder;
import org.carbondata.query.aggregator.MeasureAggregator;

/**
 * @author K00900207
 *         The sum distinct aggregator
 *         Ex:
 *         ID NAME Sales
 *         1 a 200
 *         2 a 100
 *         3 a 200
 *         select sum(distinct sales) # would result 300
 */

public class SumDistinctDoubleAggregator extends AbstractMeasureAggregatorBasic {

    /**
     *
     */
    private static final long serialVersionUID = 6313463368629960155L;

    /**
     * For Spark MOLAP to avoid heavy object transfer it better to flatten the
     * Aggregators. There is no aggregation expected after setting this value.
     */
    private Double computedFixedValue;

    /**
     *
     */
    private Set<Double> valueSet;

    public SumDistinctDoubleAggregator() {
        valueSet = new HashSet<Double>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    }

    /**
     * just need to add the unique values to agg set
     */
    @Override
    public void agg(double newVal) {
        valueSet.add(newVal);
    }

    /**
     * Distinct Aggregate function which update the Distinct set
     *
     * @param newVal new value
     */
    @Override
    public void agg(Object newVal) {
        valueSet.add(newVal instanceof Double ? (Double) newVal : new Double(newVal.toString()));
    }

    @Override
    public void agg(MolapReadDataHolder newVal, int index) {
        valueSet.add(newVal.getReadableDoubleValueByIndex(index));
    }

    /**
     * Below method will be used to get the value byte array
     */
    @Override
    public byte[] getByteArray() {
        Iterator<Double> iterator = valueSet.iterator();
        ByteBuffer buffer =
                ByteBuffer.allocate(valueSet.size() * MolapCommonConstants.DOUBLE_SIZE_IN_BYTE);
        // CHECKSTYLE:OFF Approval No:Approval-V3R8C00_018
        while (iterator.hasNext()) { // CHECKSTYLE:ON
            buffer.putDouble(iterator.next());
        }
        buffer.rewind();
        return buffer.array();
    }

    private void agg(Set<Double> set2) {
        valueSet.addAll(set2);
    }

    /**
     * merge the valueset so that we get the count of unique values
     */
    @Override
    public void merge(MeasureAggregator aggregator) {
        SumDistinctDoubleAggregator distinctAggregator = (SumDistinctDoubleAggregator) aggregator;
        agg(distinctAggregator.valueSet);
    }

    @Override
    public Double getDoubleValue() {
        if (computedFixedValue == null) {
            double result = 0;
            for (Double aValue : valueSet) {
                result += aValue;
            }
            return result;
        }
        return computedFixedValue;
    }

    @Override
    public Object getValueObject() {
        return getDoubleValue();
    }

    /**
     * @see MeasureAggregator#setNewValue(Object)
     */
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
    public void writeData(DataOutput dataOutput) throws IOException {
        if (computedFixedValue != null) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 8);
            byteBuffer.putInt(-1);
            byteBuffer.putDouble(computedFixedValue);
            byteBuffer.flip();
            dataOutput.write(byteBuffer.array());
        } else {
            int length = valueSet.size() * 8;
            ByteBuffer byteBuffer = ByteBuffer.allocate(length + 4 + 1);
            byteBuffer.putInt(length);
            for (double val : valueSet) {
                byteBuffer.putDouble(val);
            }
            byteBuffer.flip();
            dataOutput.write(byteBuffer.array());
        }
    }

    @Override
    public void readData(DataInput inPut) throws IOException {
        int length = inPut.readInt();

        if (length == -1) {
            computedFixedValue = inPut.readDouble();
            valueSet = null;
        } else {
            length = length / 8;
            valueSet = new HashSet<Double>(length + 1, 1.0f);
            for (int i = 0; i < length; i++) {
                valueSet.add(inPut.readDouble());
            }
        }
    }

    @Override
    public void merge(byte[] value) {
        if (0 == value.length) {
            return;
        }
        ByteBuffer buffer = ByteBuffer.wrap(value);
        buffer.rewind();
        while (buffer.hasRemaining()) {
            agg(buffer.getDouble());
        }
    }

    public String toString() {
        if (computedFixedValue == null) {
            return valueSet.size() + "";
        }
        return computedFixedValue + "";
    }

    @Override
    public MeasureAggregator getCopy() {
        SumDistinctDoubleAggregator aggregator = new SumDistinctDoubleAggregator();
        aggregator.valueSet = new HashSet<Double>(valueSet);
        return aggregator;
    }

    @Override
    public int compareTo(MeasureAggregator msr) {
        double msrValObj = getDoubleValue();
        double otherVal = msr.getDoubleValue();
        if (msrValObj > otherVal) {
            return 1;
        }
        if (msrValObj < otherVal) {
            return -1;
        }
        return 0;
    }
}
