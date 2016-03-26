/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdEVzw1icjfRowqz2DW4XzUpEhhSzBOwVynEHjc
u0090WB80p6C2F0BaV/nPfET1YH8WiTmzGmqlGWNJnfkabT2Vmafa4wfhnkaSIrrXKAvI9Ss
fyGrpchxXf0OpbQu1yNmt8TEQOXGMOJXYTrPMP0HALMZhWitjz8eQcofUoPkTw==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package org.carbondata.query.aggregator.impl;

import java.math.BigDecimal;

import org.carbondata.core.datastorage.store.dataholder.MolapReadDataHolder;
import org.carbondata.query.aggregator.MeasureAggregator;

/**
 * AbstractMeasureAggregatorMaxMin
 * Used for custom Molap Aggregator max  min
 */
public abstract class AbstractMeasureAggregatorMaxMin implements MeasureAggregator {
    private static final long serialVersionUID = 1L;

    protected Comparable<Object> aggVal;

    protected boolean firstTime = true;

    protected abstract void internalAgg(Object value);

    @Override public void agg(double newVal) {
        internalAgg((Double) newVal);
        firstTime = false;
    }

    @Override public void agg(Object newVal) {
        internalAgg(newVal);
        firstTime = false;
    }

    @Override public void agg(MolapReadDataHolder newVal, int index) {
        internalAgg(newVal.getReadableDoubleValueByIndex(index));
        firstTime = false;
    }

    @Override public Double getDoubleValue() {
        return (Double) ((Object) aggVal);
    }

    @Override public Long getLongValue() {
        return (Long) ((Object) aggVal);
    }

    @Override public BigDecimal getBigDecimalValue() {
        return (BigDecimal) ((Object) aggVal);
    }

    /**
     * @see MeasureAggregator#setNewValue(Object)
     */
    @Override public void setNewValue(Object newValue) {
        //        aggVal= newValue;
    }

    /**
     * This method return the max value as an object
     *
     * @return max value as an object
     */
    @Override public Object getValueObject() {
        return aggVal;
    }

    @Override public boolean isFirstTime() {
        return firstTime;
    }

    @Override public MeasureAggregator get() {
        return this;

    }

    public String toString() {
        return aggVal + "";
    }

    @Override public int compareTo(MeasureAggregator msrAggr) {
        @SuppressWarnings("unchecked") Comparable<Object> other =
                (Comparable<Object>) msrAggr.getValueObject();

        return aggVal.compareTo(other);
    }
}
