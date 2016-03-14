/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2012
 * =====================================
 *
 */
package com.huawei.unibi.molap.engine.datastorage;


/**
 * 
 * @author K00900841
 *
 */
public class CubeSliceLoader
{
    
    private static CubeSliceLoader instance = new CubeSliceLoader();

    /**
     * Create instance for calling of action sequence.
     * 
     * @return added by liupeng 00204190
     */
    public static CubeSliceLoader getInstance()
    {
        return instance;
    }
    
    public void loadSliceFromFiles(String filesLocaton)
    {
    }
    public void updateSlices(String newSlicePath,String[] slicePathsToDelete)
    {
        
    }
}
