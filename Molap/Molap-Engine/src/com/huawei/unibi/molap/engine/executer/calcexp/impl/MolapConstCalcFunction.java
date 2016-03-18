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
