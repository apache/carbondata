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
package com.huawei.datasight.molap.partition.api;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.huawei.unibi.molap.query.metadata.MolapDimensionLevelFilter;

public interface Partition extends Serializable
{
    /**
     * unique identification for the partition in the cluster.
     */
    String getUniqueID();

    /**
     * File path for the raw data represented by this partition 
     */
    String getFilePath();
    
    /**
     * result 
     * @return
     */
    List<String> getFilesPath();

    /**
     * Column name and constraints used to distribute the data
     */
    Map<String, MolapDimensionLevelFilter> getPartitionDetails();

}