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

import java.util.HashMap;
import java.util.Map;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.util.MolapProperties;

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : Molap Data Processor 
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM 
 * FileName :CheckPointHanlder.java 
 * Class Description :CheckPointHanlder 
 * Version 1.0
 */
public final class CheckPointHanlder
{
    private CheckPointHanlder()
    {
        
    }
    /**
     * IS_CHECK_POINT_NEEDED
     */
    public static final boolean IS_CHECK_POINT_NEEDED = Boolean
            .valueOf(MolapProperties
                    .getInstance()
                    .getProperty(
                            MolapCommonConstants.MOLAP_DATALOAD_CHECKPOINT,
                            MolapCommonConstants.MOLAP_DATALOAD_CHECKPOINT_DEFAULTVALUE));
    /**
     * dummyCheckPoint
     */
    private static final CheckPointInterface DUMMYCHECKPOINT = new DummyCheckPointHandler();
    
    /**
     * check point cache 
     */
    private static Map<String, CheckPointInterface> checkpoints = new HashMap<String, CheckPointInterface>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    
    /**
     * Below method will be used to initialise the check point
     * @param transPath
     * @param checkPointType
     * @return check point 
     */
    public static void initializeCheckpoint(String transName,
            CheckPointType checkPointType,String checkPointFileLocation,String checkPointFileName)
    {
        // if check is not true 
        if(IS_CHECK_POINT_NEEDED)
        {
            // if present in case return else create and return
            CheckPointInterface checkPoint = checkpoints.get(transName);
            if(null == checkPoint && checkPointType.equals(CheckPointType.CSV))
            {
                checkPoint = new CSVCheckPointHandler(checkPointFileLocation,checkPointFileName);
                checkpoints.put(transName, checkPoint);
            }
        }
    }
    
    /**
    * Below method will be used to get the check point from cache 
    * @param transPath
    * @return CheckPoint
    */
   public static CheckPointInterface getCheckpoint(String transPath)
   {
       if(!IS_CHECK_POINT_NEEDED)
       {
           return DUMMYCHECKPOINT;
       }
       CheckPointInterface checkPoint = checkpoints.get(transPath);
       return null != checkPoint ? checkPoint : DUMMYCHECKPOINT;
   }
   
   /**
    * Below method will be used to get the dummy check point 
    * @return dummy check point
    */
 
   public static CheckPointInterface getDummyCheckPoint()
   {
       return DUMMYCHECKPOINT;
   }
}
