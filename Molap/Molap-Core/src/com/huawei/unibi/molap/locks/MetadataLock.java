package com.huawei.unibi.molap.locks;

import java.io.File;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
/**
 * 
 * @author R00903928
 *
 * This class is responsible for locking of the load metadata.
 */
public class MetadataLock extends MolapLock
{
    
    /**
     * This is used for locking of the load metadata.
     * @param location
     */
    public MetadataLock(String location)
    {
        String loadLock = location + File.separator + MolapCommonConstants.METADATA_LOCK;
        this.location = loadLock;
        initRetry();
    }

}
