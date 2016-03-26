/**
 *
 */
package org.carbondata.query.aggregator.impl;

import org.carbondata.core.datastorage.store.dataholder.MolapReadDataHolder;

/**
 * @author z00305190
 */
public class DummyDoubleAggregator extends AbstractMeasureAggregatorDummy {
    private static final long serialVersionUID = 1L;

    /**
     * aggregate value
     */
    private double aggVal;

    @Override
    public void agg(double newVal) {
        aggVal = newVal;
    }

    @Override
    public void agg(Object newVal) {
        aggVal = (Double) newVal;
    }

    @Override
    public void agg(MolapReadDataHolder newVal, int index) {
        aggVal = newVal.getReadableDoubleValueByIndex(index);
    }

    @Override
    public Double getDoubleValue() {
        return aggVal;
    }

    @Override
    public Object getValueObject() {
        // TODO Auto-generated method stub
        return aggVal;
    }

    @Override
    public void setNewValue(Object newValue) {
        // TODO Auto-generated method stub
        aggVal = (Double) newValue;
    }
}
