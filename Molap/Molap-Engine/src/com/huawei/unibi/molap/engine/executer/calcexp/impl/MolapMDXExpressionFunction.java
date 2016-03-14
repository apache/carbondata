/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcAIRTtLWBkMMN+iqJ62JNQb/MYFaBoemC1VlrU
n+vkORYvBw+w0AsNztzlzsFFhFvywchGJeuHl2AF/54xYYdkWOykUFpiK+hwPNVEa0EMMrGo
oz8EjnfBBggwpWZxtmi1WPKuJJshokT3nB/wa6G+HDtyCvDEZE9zUowdAD79SQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.engine.executer.calcexp.impl;

//import mondrian.mdx.ResolvedFunCall;
//import mondrian.olap.Exp;

//Expression ResolveFunctionCall, Literal, MemberExp

import com.huawei.unibi.molap.olap.Exp;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;

/**
 * This class for Evaluating MDX expression.
 * 
 * @author V00900840
 *
 */
public class MolapMDXExpressionFunction extends AbstractMolapCalcFunction
{

    /**
     * 
     */
    private static final long serialVersionUID = -4868338173011000925L;

    @Override
    public double calculate(MeasureAggregator[] msrAggs)
    {
        double left = leftOperand.calculate(msrAggs);
        double right = rightOperand.calculate(msrAggs);
        return conditionValue ? left : right;
    }
    
    
    @Override
    public void compile(CalcExpressionModel model, Exp exp)
    {
//        ResolvedFunCall funCall = (ResolvedFunCall)exp;
//        Exp[] args = funCall.getArgs();
//        getOperand(args[0], model);
//        leftOperand = getOperand(args[1],model);
//        rightOperand = getOperand(args[2], model);
    }
}
