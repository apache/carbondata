/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcAIRTtLWBkMMN+iqJ62JNQb/MYFaBoemC1VlrU
n+vkOS5I5wEaPYR6dJ6AgKtFShsmkL33qt1+djDaDpwyslG6VTSzAD6qKbVoRDJ/NBFbwDvq
ERpya8oqRdn24SfYf9EUPO7KwsKEz5UYv1WC9o+WWMLtWSRgo9/cDyxlISpCmw==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.calcexp.impl;

//import mondrian.mdx.ResolvedFunCall;
//import mondrian.olap.Exp;

import com.huawei.unibi.molap.olap.Exp;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;

/**
 * @author R00900208
 *
 */
public class MolapNegetiveFunction extends AbstractMolapCalcFunction
{
    

    /**
     * 
     */
    private static final long serialVersionUID = -5262752389398212774L;

    /**
     * @param index
     */
    public MolapNegetiveFunction()
    {
    }


    @Override
    public double calculate(MeasureAggregator[] msrAggs)
    {
        return -(leftOperand.calculate(msrAggs));
    }
    
    @Override
    public void compile(CalcExpressionModel model, Exp exp)
    {
//        ResolvedFunCall funCall = (ResolvedFunCall)exp;
//        Exp[] args = funCall.getArgs();
//        leftOperand = getOperand(args[0],model);
    }

}
