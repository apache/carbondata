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

package org.carbondata.query.executer.calcexp.impl;

import org.carbondata.core.olap.Exp;
import org.carbondata.query.aggregator.MeasureAggregator;

//import mondrian.mdx.MemberExpr;
//import mondrian.olap.Exp;
//import mondrian.rolap.RolapBaseCubeMeasure;

public class MolapSingleMeasureFunction extends AbstractMolapCalcFunction {

    /**
     *
     */
    private static final long serialVersionUID = -8056333924830146155L;
    private int index;

    /**
     * @param index
     */
    public MolapSingleMeasureFunction() {
    }

    @Override public double calculate(MeasureAggregator[] msrAggs) {
        double value;
        if (msrAggs[index].toString().contains("Long")) {
            value = msrAggs[index].getLongValue();
        } else if (msrAggs[index].toString().contains("Decimal")) {
            value = msrAggs[index].getBigDecimalValue().doubleValue();
        } else {
            value = msrAggs[index].getDoubleValue();
        }

        return msrAggs[index].isFirstTime() ? 0 : value;
    }

    @Override public void compile(CalcExpressionModel model, Exp exp) {
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
