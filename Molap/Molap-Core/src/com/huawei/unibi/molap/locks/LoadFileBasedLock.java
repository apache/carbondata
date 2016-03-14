package com.huawei.unibi.molap.locks;

import java.io.File;

import com.huawei.unibi.molap.constants.MolapCommonConstants;

/**
 * 
 * @author R00903928
 *
 * This class is responsible for locking of the load .
 */
public class LoadFileBasedLock extends MolapLock 
{
    /**
     * This is used for locking of the load.
     * @param location
     */
    public LoadFileBasedLock(String location)
    {
        String loadLock = location + File.separator + MolapCommonConstants.LOAD_LOCK;
        this.location = loadLock;
        initRetry();
    }

}
