/**
 *
 */
package org.carbondata.query.aggregator.impl;

import org.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;

/**
 * @author z00305190
 */
public class DummyLongAggregator extends AbstractMeasureAggregatorDummy {
    private static final long serialVersionUID = 1L;

    /**
     * aggregate value
     */
    private long aggVal;

    @Override
    public void agg(Object newVal) {
        aggVal = (Long) newVal;
    }

    @Override
    public void agg(CarbonReadDataHolder newVal, int index) {
        aggVal = newVal.getReadableLongValueByIndex(index);
    }

    @Override
    public Long getLongValue() {
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
        aggVal = (Long) newValue;
    }
}
