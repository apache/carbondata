/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdEVzw1icjfRowqz2DW4XzUpEhhSzBOwVynEHjc
u0090WB80p6C2F0BaV/nPfET1YH8WiTmzGmqlGWNJnfkabT2Vmafa4wfhnkaSIrrXKAvI9Ss
fyGrpchxXf0OpbQu1yNmt8TEQOXGMOJXYTrPMP0HALMZhWitjz8eQcofUoPkTw==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package org.carbondata.query.aggregator.impl;

import java.math.BigDecimal;

import org.carbondata.query.aggregator.MeasureAggregator;

/**
 * AbstractMeasureAggregatorSum
 * Used for custom Molap Aggregator sum
 */
public abstract class AbstractMeasureAggregatorBasic implements MeasureAggregator {
    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 1L;

    protected boolean firstTime = true;

    @Override
    public void agg(double newVal) {
        //        valueSet.add(newVal);
    }

    @Override
    public Double getDoubleValue() {
        return null;
    }

    @Override
    public Long getLongValue() {
        return null;
    }

    @Override
    public BigDecimal getBigDecimalValue() {
        return null;
    }

    @Override
    public boolean isFirstTime() {
        return firstTime;
    }

    @Override
    public MeasureAggregator get() {
        return this;
    }
}
