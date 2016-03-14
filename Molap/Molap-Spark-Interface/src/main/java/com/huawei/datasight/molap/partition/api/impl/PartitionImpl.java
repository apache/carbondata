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
package com.huawei.datasight.molap.partition.api.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.huawei.datasight.molap.partition.api.Partition;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.query.metadata.MolapDimensionLevelFilter;

/**
 * 
 * @author K00900207
 *
 */
public class PartitionImpl implements Partition
{
    /**
     * 
     * Comment for <code>serialVersionUID</code>
     * 
     */
    private static final long serialVersionUID = 3020172346383028547L;
    private String uniqueID;
    private String folderPath;
    
    private Map<String, MolapDimensionLevelFilter> filterMap = new HashMap<String, MolapDimensionLevelFilter>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE); 
    
    public PartitionImpl(String uniqueID, String folderPath)
    {
        this.uniqueID =uniqueID;
        this.folderPath =folderPath;
    }

    /**
     * 
     * @see com.huawei.datasight.molap.partiion.api.Partition#getUniqueID()
     * 
     */
    @Override
    public String getUniqueID()
    {
        return uniqueID;
    }

    /**
     * 
     * @see com.huawei.datasight.molap.partiion.api.Partition#getFilePath()
     * 
     */
    @Override
    public String getFilePath()
    {
        return folderPath;
    }

    /**
     * 
     * @see com.huawei.datasight.molap.partiion.api.Partition#getPartitionDetails()
     * 
     */
    @Override
    public Map<String, MolapDimensionLevelFilter> getPartitionDetails()
    {
        return filterMap;
    }
    
    public void setPartitionDetails(String columnName, MolapDimensionLevelFilter filter)
    {
        filterMap.put(columnName, filter);
    }
    
    @Override
    public String toString()
    {
        return "{PartitionID -> "+ uniqueID + " Path: " + folderPath + '}';
    }

    @Override
    public List<String> getFilesPath()
    {
        // TODO Auto-generated method stub
        return null;
    }

}
