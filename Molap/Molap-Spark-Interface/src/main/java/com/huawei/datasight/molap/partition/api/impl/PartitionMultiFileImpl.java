package com.huawei.datasight.molap.partition.api.impl;

import java.util.List;
import java.util.Map;

import com.huawei.datasight.molap.partition.api.Partition;
import com.huawei.unibi.molap.query.metadata.MolapDimensionLevelFilter;

public class PartitionMultiFileImpl implements Partition
{

    
    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = -4363447826181193976L;
    private String uniqueID;
    private List<String> folderPath;
    
    public PartitionMultiFileImpl(String uniqueID, List<String> folderPath)
    {
        this.uniqueID =uniqueID;
        this.folderPath =folderPath;
    }
    
    @Override
    public String getUniqueID()
    {
        // TODO Auto-generated method stub
        return uniqueID;
    }

    @Override
    public String getFilePath()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<String, MolapDimensionLevelFilter> getPartitionDetails()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<String> getFilesPath()
    {
        // TODO Auto-generated method stub
        return folderPath;
    }

}
