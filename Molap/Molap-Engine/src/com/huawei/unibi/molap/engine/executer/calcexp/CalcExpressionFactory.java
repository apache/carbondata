/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcAIRTtLWBkMMN+iqJ62JNQb/MYFaBoemC1VlrU
n+vkOQ17WnuynG1SWGdkqVhUij9YVsw9qXmbfF7IFe6Cl0B17o3hX+vUJUUqZRRTE595Z7ip
IyeyQrC3vEJqGSk9VUZ/Wnxku2asACtJtO+yKXB8DQDCK/8Jp0Jv2dJZ56OEDQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.calcexp;

import com.huawei.unibi.molap.engine.executer.calcexp.impl.MolapAddFunction;
import com.huawei.unibi.molap.engine.executer.calcexp.impl.MolapDivideFunction;
import com.huawei.unibi.molap.engine.executer.calcexp.impl.MolapEqualFunction;
import com.huawei.unibi.molap.engine.executer.calcexp.impl.MolapMDXExpressionFunction;
import com.huawei.unibi.molap.engine.executer.calcexp.impl.MolapMultiplyFunction;
import com.huawei.unibi.molap.engine.executer.calcexp.impl.MolapParenthesisFunction;
import com.huawei.unibi.molap.engine.executer.calcexp.impl.MolapSingleMeasureFunction;
import com.huawei.unibi.molap.engine.executer.calcexp.impl.MolapSubtractionFunction;

/**
 * @author R00900208
 * Factory class for calc expressions
 */
public final class CalcExpressionFactory
{
    
    private static CalcExpressionFactory instance;
    
    private CalcExpressionFactory()
    {
        
    }
    
    /**
     * Creates the instance of factory.
     * @return
     */
    public static synchronized CalcExpressionFactory getInstance()
    {
        if(null == instance)
        {
            instance = new CalcExpressionFactory();
        }
        return instance;
    }
    
    /**
     * Creates calc function.
     * @param name
     * @return
     */
    public MolapCalcFunction getCalcFunction(CalCExpressionName name)
    {
        if(name == null)
        {
            return null;
        }
        switch(name)
        {
        case ADD :
            return new MolapAddFunction();
        case DIVIDE : 
            return new MolapDivideFunction();
        case NEGATIVE : 
            return new MolapSubtractionFunction();
        case MULTIPLY : 
            return new MolapMultiplyFunction();
        case PARENTHESIS : 
            return new MolapParenthesisFunction();
        case IIf :
            return new MolapMDXExpressionFunction();
        case EQUAL :
            return new MolapEqualFunction();
        default :
            return null;
        }
    }
    
    /**
     * Creates the measure function.
     * @return
     */
    public MolapCalcFunction getSingleCalcFunction()
    {
        return new MolapSingleMeasureFunction();
    }

}
