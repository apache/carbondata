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
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbweRARwUrjYxPx0CUk3mVB7mxOcZSaagKrMQNlhB
QO/t7BcAJpLIKe6S5BNCsI2XXRyMrW3ijjKxOtUGUzFsjySSXKmKxOsMZ1Bq+2ofjXvvKUdR
Uemn3MOaDIOB3LU/KMSrmay7H7pKAhBi/fWXLrRlL4WGUNumjg5iks5crQOmiw==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.calcexp.impl;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
//import mondrian.mdx.MemberExpr;
//import mondrian.olap.Exp;
//import mondrian.rolap.RolapBaseCubeMeasure;
import com.huawei.unibi.molap.olap.Exp;

/**
 * @author R00900208
 *
 */
public class MolapSingleMeasureFunction extends AbstractMolapCalcFunction
{
    
    /**
     * 
     */
    private static final long serialVersionUID = -8056333924830146155L;
    private int index;
    
    

    /**
     * @param index
     */
    public MolapSingleMeasureFunction()
    {
    }



    @Override
    public double calculate(MeasureAggregator[] msrAggs)
    {
        return msrAggs[index].isFirstTime()?0:msrAggs[index].getValue();
    }

    @Override
    public void compile(CalcExpressionModel model, Exp exp)
    {
//        MemberExpr expr = (MemberExpr)exp;
//        RolapBaseCubeMeasure cubeMeasure = (RolapBaseCubeMeasure)expr.getMember();
//        List<Measure> msrsList = model.getMsrsList();
//        int i = 0;
//        for(Measure measure : msrsList)
//        {
//            if(cubeMeasure.getName().equals(measure.getName()))
//            {
//                break;
//            }
//            i++;
//        }
//        index = i;
    }

}
