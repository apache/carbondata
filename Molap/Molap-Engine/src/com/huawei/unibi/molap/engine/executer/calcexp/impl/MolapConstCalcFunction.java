/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcAIRTtLWBkMMN+iqJ62JNQb/MYFaBoemC1VlrU
n+vkOU1eIbk9fzWZVFADzWif7bZJWzsfEjxRiXK+lBixluT67IehLFctk//PNNo79H3xmRFS
Q9IYPyRZO3AmFtdF1IhUzsv+tMh5VRrMm3xOdDFdXDMeUzOvyyA1CgtQTrjIHQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.calcexp.impl;

//import mondrian.olap.Exp;
//import mondrian.olap.Literal;

import com.huawei.unibi.molap.olap.Exp;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;

/**
 * @author R00900208
 *
 */
public class MolapConstCalcFunction extends AbstractMolapCalcFunction
{
    /**
     * 
     */
    private static final long serialVersionUID = -3394986838338896821L;
    private double literal; 

    @Override
    public double calculate(MeasureAggregator[] msrAggs)
    {
        return literal;
    }
    
    @Override
    public void compile(CalcExpressionModel model, Exp exp)
    {
//        Literal expr = (Literal)exp;
//        literal = expr.getIntValue();
        
    }

}
