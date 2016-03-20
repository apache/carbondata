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

package com.huawei.unibi.molap.engine.executer.calcexp.impl;


import com.huawei.unibi.molap.olap.Exp;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;

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
