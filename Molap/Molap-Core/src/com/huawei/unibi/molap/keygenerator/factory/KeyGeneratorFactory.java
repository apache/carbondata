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

