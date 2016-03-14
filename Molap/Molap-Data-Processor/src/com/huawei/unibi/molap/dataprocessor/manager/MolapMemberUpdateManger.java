package com.huawei.unibi.molap.dataprocessor.manager;

import java.util.HashMap;
import java.util.Map;

import com.huawei.unibi.molap.constants.MolapCommonConstants;

public final class MolapMemberUpdateManger
{
    /**
     * instance
     */
    private static final MolapMemberUpdateManger INSTANCE = new MolapMemberUpdateManger();
    
    /**
     * managerHandlerMap
     */
    private Map<String,Object> managerHandlerMap;
    
    /**
     * private constructor
     */
    private MolapMemberUpdateManger()
    {
        managerHandlerMap= new HashMap<String, Object>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    }
    
    /**
     * Get instance method will be used to get the class instance 
     * @return
     */
    public static MolapMemberUpdateManger getInstance()
    {
        return INSTANCE;
    }
    

    /**
     * Below method will be used to get the lock object for all the data processing request.
     * form the local map, if empty than it will update the map and return the lock object
     * @param key
     * @return
     */
    public synchronized Object getDataProcessingLockObject(String key)
    {
        Object object = managerHandlerMap.get(key);
        if(null==object)
        {
            object = new Object();
            managerHandlerMap.put(key, object);
        }
        return object;
    }
}
