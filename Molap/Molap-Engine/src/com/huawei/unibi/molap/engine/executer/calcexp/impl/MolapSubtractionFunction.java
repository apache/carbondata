/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbweRARwUrjYxPx0CUk3mVB7mxOcZSaagKrMQNlhB
QO/t7FpvXOq29ZVFmJ8UFqSsWL9nt7Wq33Xx47hkveX+euJX/xxVWLgAQe/TKwZSHnQhKgKA
dsxWlqoxbOjSj6gaumSb8HsZKPFXWBvUgFHO8j0EYHaOtcxKkWXqA+jdAdulHQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.calcexp.impl;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;

/**
 * MolapSubtractionFunction
 * @author R00900208
 *
 */
public class MolapSubtractionFunction extends AbstractMolapCalcFunction
{
    
    /**
     * 
     */
    private static final long serialVersionUID = 8480394704920748842L;

    @Override
    public double calculate(MeasureAggregator[] msrAggs)
    {
        double left = leftOperand.calculate(msrAggs);
        double right = null!=rightOperand?rightOperand.calculate(msrAggs):0.0;
        return left-right;
    }
}
