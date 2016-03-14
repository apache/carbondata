/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 1997
 * =====================================
 *
 */
package com.huawei.unibi.molap.constants;

import com.huawei.unibi.molap.util.MolapProperties;

/**
 * 
 * @author K00900207
 *
 */
public final class DataSetUtil
{
    private DataSetUtil()
    {
        
    }
    public static final  String DATA_SET_LOCATION = MolapProperties.getInstance().getProperty("spark.dataset.location", "../datasets/");
    
    public static final String DP_LOCATION = MolapProperties.getInstance().getProperty("spark.dp.location", "../datapipelines/");
    
    public static final String DATA_SOURCE_LOCATION = MolapProperties.getInstance().getProperty("spark.sqlconnections.location", "../unibi-solutions/system/dbconnection/sqlconnections.xml");

}
