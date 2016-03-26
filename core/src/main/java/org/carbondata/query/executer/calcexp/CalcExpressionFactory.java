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

package org.carbondata.query.executer.calcexp;

import org.carbondata.query.executer.calcexp.impl.*;

/**
 * Factory class for calc expressions
 */
public final class CalcExpressionFactory {

    private static CalcExpressionFactory instance;

    private CalcExpressionFactory() {

    }

    /**
     * Creates the instance of factory.
     *
     * @return
     */
    public static synchronized CalcExpressionFactory getInstance() {
        if (null == instance) {
            instance = new CalcExpressionFactory();
        }
        return instance;
    }

    /**
     * Creates calc function.
     *
     * @param name
     * @return
     */
    public MolapCalcFunction getCalcFunction(CalCExpressionName name) {
        if (name == null) {
            return null;
        }
        switch (name) {
        case ADD:
            return new MolapAddFunction();
        case DIVIDE:
            return new MolapDivideFunction();
        case NEGATIVE:
            return new MolapSubtractionFunction();
        case MULTIPLY:
            return new MolapMultiplyFunction();
        case PARENTHESIS:
            return new MolapParenthesisFunction();
        case IIf:
            return new MolapMDXExpressionFunction();
        case EQUAL:
            return new MolapEqualFunction();
        default:
            return null;
        }
    }

    /**
     * Creates the measure function.
     *
     * @return
     */
    public MolapCalcFunction getSingleCalcFunction() {
        return new MolapSingleMeasureFunction();
    }

}
