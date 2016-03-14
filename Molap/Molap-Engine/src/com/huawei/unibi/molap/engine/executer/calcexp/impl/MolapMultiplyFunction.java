/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcAIRTtLWBkMMN+iqJ62JNQb/MYFaBoemC1VlrU
n+vkOezC0nV8lF+1AbFyXDK6jxOSyn+ymekWK3cU0xbpBiw/Hox6c55YCELUsE1noyZigr6/
jCB5MDTYIBzXHYFCaXTtjx3AGq6I8mg7+QGW4e7BjU+myXz5oTgXiCFnDwKSWA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.calcexp.impl;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;

/**
 * It multiplies the operands.
 * @author R00900208
 *
 */
public class MolapMultiplyFunction extends AbstractMolapCalcFunction
{
    

    /**
     * 
     */
    private static final long serialVersionUID = -4372447898911845753L;

    @Override
    public double calculate(MeasureAggregator[] msrAggs)
    {
        double left = leftOperand.calculate(msrAggs);
        double right = null!=rightOperand?rightOperand.calculate(msrAggs):1.0;
        return left*right;
    }
    
    

}
