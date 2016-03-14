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

import java.util.List;

import org.apache.spark.sql.cubemodel.Partitioner;

import com.huawei.datasight.molap.query.MolapQueryPlan;

/**
 * 
 * @author K00900207
 *
 */
public interface DataPartitioner
{
    /**
     * Initialise the partitioner based on the given columns
     * 
     * @param basePath
     * @param columns
     * @param properties
     * 
     */
    void initialize(String basePath, String[] columns, Partitioner partitioner);
    
    /**
     * All the partitions built by the Partitioner
     */
    List<Partition> getAllPartitions();
    
    /**
     * Partition where the tuple should be present. (API used for data loading purpose)
     */
    Partition getPartionForTuple(Object[] tuple, long rowCounter);
    
    /**
     * Identifies the partitions applicable for the given filter (API used for For query) 
     */
    List<Partition> getPartitions(MolapQueryPlan queryPlan);
    
    /**
     * 
     * @return partitioned columns
     */
    String[] getPartitionedColumns();
    
    Partitioner getPartitioner();
    
    
}

