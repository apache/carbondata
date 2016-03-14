/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcAIRTtLWBkMMN+iqJ62JNQb/MYFaBoemC1VlrU
n+vkOWwJdPbFpF99SHDgcrZhtFb9kqvPCoXnarju4+PDIo47TqwibDvKFzMA2rs9Qu/szbXr
zrhuP7RwRyFHUNLYs//y0K3UDdXWBlrSAgK+TplEI38FlsSm4x3wqSxrL7MlgQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.calcexp;

import java.io.Serializable;

//import mondrian.olap.Exp;
import com.huawei.unibi.molap.olap.Exp;



import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.executer.calcexp.impl.CalcExpressionModel;

/**
 * This calculates the calculated measures.
 * 
 * @author R00900208
 *
 */
public interface MolapCalcFunction extends Serializable
{

    /**
     * Calculate the function by using aggregates
     * @param msrAggs
     * @return
     */
    double calculate(MeasureAggregator[] msrAggs);
    
    /**
     * 
     * @param model
     * @param exp
     */
    void compile(CalcExpressionModel model, Exp exp);
    
}
