/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcAIRTtLWBkMMN+iqJ62JNQb/MYFaBoemC1VlrU
n+vkObtMrIkVNQ2GczamWW1ZDXq2L8mCHFTCZHvilLSOnUXAk8j6v4dUFFl9d9j9FS8JuoXy
UmpM1DdovZiGkZAOjgdzfFC9XR3ckz2xFSvmTnxqHOnDs2+qIcMIQwCLaxBzkA==*/
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
public class MolapDivideFunction extends AbstractMolapCalcFunction
{

    
    /**
     * 
     */
    private static final long serialVersionUID = -4919973684249918437L;

    /**
     * @param leftOperand
     * @param rightOperand
     */
    public MolapDivideFunction()
    {

    }

    @Override
    public double calculate(MeasureAggregator[] msrAggs)
    {
        double left = leftOperand.calculate(msrAggs);
        double right = null!=rightOperand?rightOperand.calculate(msrAggs):1.0;
        return (left/right);
    }



}
