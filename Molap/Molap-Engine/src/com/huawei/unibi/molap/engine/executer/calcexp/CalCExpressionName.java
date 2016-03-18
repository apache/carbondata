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
n+vkOSmkmyhfVYpwuT2L6ZIwvFI/LYafLDnEFspGED5L2xqM2dpnLzHr0ZdEsGlc+TJaxVTZ
5kisVXLBzDublc8l0Xqq9DdqYcCFj23/xhV3cPJ7T5LDWqVs9ntmO0rwU88cVA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.calcexp;

/**
 * @author R00900208
 *
 */
public enum CalCExpressionName 
{
    
    /**
     * Types of calculated expression defined.
     */
    ADD("+"),
    /**
     * DIVIDE.
     */
    DIVIDE("/"),
    /**
     * NEGATIVE.
     */
    NEGATIVE("-"),
    /**
     * SUBSTRACT.
     */
    SUBSTRACT("-"),
    /**
     * MULTIPLY.
     */
    MULTIPLY("*"),
    /**
     * PARENTHESIS.
     */
    PARENTHESIS("()"),
    /**
     * MDX Statement
     * 
     */
    IIf("IIf"),
    /**
     * Equal
     */
    EQUAL("=");
    
    /**
     * name.
     */
    private String name;
    
    private CalCExpressionName(String name)
    {
        this.name = name;
    }
    
    /**
     * getName.
     * @return String
     */
    public String getName()
    {
        return name;
    }

}
