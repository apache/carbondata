/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcAIRTtLWBkMMN+iqJ62JNQb/MYFaBoemC1VlrU
n+vkOY8zfCVMc9NqGMLeBHYzsW3EBmbF3Av+bOEWadTzwySqy/IuZrc3isKR5pqUHufcyjRv
nmpISKQQUYBlN3vCv1cvg1XUnTWxeOovw08umbvWhS7UHe7ly/xSHMRupAG4Ew==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.engine.executer.calcexp.impl;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;

/**
 * This class is for calculating the equal functionality.
 * 
 * @author V00900840
 *
 */
public class MolapEqualFunction extends AbstractMolapCalcFunction
{

    /**
     * 
     */
    private static final long serialVersionUID = 6534281025960495301L;

    /**
     * If true then this function returns 1 and if false then 0.
     */
    @Override
    public double calculate(MeasureAggregator[] msrAggs)
    {
        double left = leftOperand.calculate(msrAggs);
        double right = rightOperand.calculate(msrAggs);
      //CHECKSTYLE:OFF    Approval No:Approval-212
        if(left == right)//CHECKSTYLE:ON
        {
            conditionValue = true;
			return 1;
        }
        return 0;
    }

}
