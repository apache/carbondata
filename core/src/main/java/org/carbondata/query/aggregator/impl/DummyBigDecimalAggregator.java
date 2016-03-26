/**
 *
 */
package org.carbondata.query.aggregator.impl;

import java.math.BigDecimal;

import org.carbondata.core.datastorage.store.dataholder.MolapReadDataHolder;

/**
 * @author z00305190
 */
public class DummyBigDecimalAggregator extends AbstractMeasureAggregatorDummy {
    private static final long serialVersionUID = 1L;

    /**
     * aggregate value
     */
    private BigDecimal aggVal;

    @Override
    public void agg(Object newVal) {
        aggVal = (BigDecimal) newVal;
    }

    @Override
    public void agg(MolapReadDataHolder newVal, int index) {
        aggVal = newVal.getReadableBigDecimalValueByIndex(index);
    }

    @Override
    public BigDecimal getBigDecimalValue() {
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
        aggVal = (BigDecimal) newValue;
    }
}
