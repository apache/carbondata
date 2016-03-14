/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
 */
package com.huawei.unibi.molap.util;


public final class MolapVersion
{
    
    private MolapVersion()
    {
        
    }
    /*
     * This is a running version no for metadata  
     */
    public static String  getCubeVersion(){
        return "1";
    }
    
    /*
     * This is a running version no for Data file version   
     */
    public static String getDataVersion(){
        return "1";
    }
    
}
