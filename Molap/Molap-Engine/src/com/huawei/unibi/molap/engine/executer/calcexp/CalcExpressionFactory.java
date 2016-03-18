/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
