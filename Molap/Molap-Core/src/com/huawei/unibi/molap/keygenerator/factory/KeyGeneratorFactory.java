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

/**
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
*/

package com.huawei.unibi.molap.keygenerator.factory;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.keygenerator.mdkey.MultiDimKeyVarLengthGenerator;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * Project Name NSE V3R7C00 
 * Module Name : Molap
 * Author V00900840
 * Created Date :Jul 1, 2013 4:02:51 PM
 * FileName : KeyGeneratorFactory.java
 * Class Description : Factory class for generating factory.
 * Version 1.0
 */
public final class KeyGeneratorFactory
{
    private KeyGeneratorFactory()
    {
        
    }
    
    /**
     * @param dimesion
     * @return
     */
    public static KeyGenerator getKeyGenerator(int []dimesion)
    {
        int[] incrementedCardinality = null;
        boolean isFullyFilled = Boolean
                .parseBoolean(MolapCommonConstants.IS_FULLY_FILLED_BITS_DEFAULT_VALUE);
//        isFullyFilled=true;
        if(!isFullyFilled)
        {
            incrementedCardinality = MolapUtil
                    .getIncrementedCardinality(dimesion);
        }
        else
        {
            incrementedCardinality = MolapUtil
                    .getIncrementedCardinalityFullyFilled(dimesion);
        }
        return new MultiDimKeyVarLengthGenerator(incrementedCardinality);
    }
}

