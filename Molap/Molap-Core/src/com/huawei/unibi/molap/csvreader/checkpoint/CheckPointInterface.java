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

package com.huawei.unibi.molap.csvreader.checkpoint;

import java.util.Map;

import com.huawei.unibi.molap.csvreader.checkpoint.exception.CheckPointException;

/**
 * Project Name NSE V3R7C00 
 * Module Name : Molap Data Processor 
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM 
 * FileName :CheckPoint.java 
 * Class Description : CheckPointInterface class 
 * Version 1.0
 */
public interface CheckPointInterface
{
    /**
     * Below method will be used to get the checkpoint cache
     * @return check point cache
     * @throws CheckPointException
     *          will throw exception in case of any error while getting the cache
     */
     Map<String,Long> getCheckPointCache() throws CheckPointException;
    
    /**
     * Below method will be used to store the check point cache 
     * 
     * @param checkPointCache
     *          check point cache 
     * 
     * @throws CheckPointException
     *          problem while storing the checkpoint cache
     */
    void saveCheckPointCache(Map<String,Long> checkPointCache) throws CheckPointException;
    
    /**
     * Below method will be used to get the check point info field count
     * @return
     */
     int getCheckPointInfoFieldCount();
    
    /**
     * This methods implementation will update the output row and add the Filepath and offset for 
     * next step. 
     * @param inputRow
     * @param outputRow
     */
     void updateInfoFields(Object[] inputRow, Object[] outputRow);
}
