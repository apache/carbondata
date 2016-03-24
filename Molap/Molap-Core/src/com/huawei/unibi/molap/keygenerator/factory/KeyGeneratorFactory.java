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

package com.huawei.unibi.molap.keygenerator.factory;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.keygenerator.mdkey.MultiDimKeyVarLengthGenerator;
import com.huawei.unibi.molap.util.MolapUtil;

public final class KeyGeneratorFactory {
    private KeyGeneratorFactory() {

    }

    public static KeyGenerator getKeyGenerator(int[] dimesion) {
        int[] incrementedCardinality = null;
        boolean isFullyFilled =
                Boolean.parseBoolean(MolapCommonConstants.IS_FULLY_FILLED_BITS_DEFAULT_VALUE);
        if (!isFullyFilled) {
            incrementedCardinality = MolapUtil.getIncrementedCardinality(dimesion);
        } else {
            incrementedCardinality = MolapUtil.getIncrementedCardinalityFullyFilled(dimesion);
        }
        return new MultiDimKeyVarLengthGenerator(incrementedCardinality);
    }
    public static KeyGenerator getKeyGenerator(int[] dimCardinality,int[][] splits)
    {
        int[] dimsBitLens = MolapUtil.getDimensionBitLength(dimCardinality, splits);
        
        return new MultiDimKeyVarLengthGenerator(dimsBitLens);
    }
}

