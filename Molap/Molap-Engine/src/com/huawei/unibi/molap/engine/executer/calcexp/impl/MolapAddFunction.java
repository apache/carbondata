/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcAIRTtLWBkMMN+iqJ62JNQb/MYFaBoemC1VlrU
n+vkOf5nLF5HJaHwAHFCwBFGTf8SJZRo2nDtq2eUp/czxgNVjyDU+q2y7VbN6PbP+Sh3ljly
/MbqJfOqDJ4+7Zo5x76x5MrC7V7ojLRl1bfy7r8a+MDP1F7K7SAh/uY5+4Tb+A==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.calcexp.impl;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;

/**
 * @author R00900208
 *
 */
public class MolapAddFunction extends AbstractMolapCalcFunction
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
        return left+right;
    }

}
