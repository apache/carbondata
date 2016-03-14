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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.cubemodel.Partitioner;

import com.huawei.datasight.molap.partition.api.DataPartitioner;
import com.huawei.datasight.molap.partition.api.Partition;
import com.huawei.datasight.molap.query.MolapQueryPlan;
import com.huawei.datasight.molap.spark.util.MolapSparkInterFaceLogEvent;
import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.query.metadata.MolapDimensionLevelFilter;

/**
 * 
 * Single partition creator for non partitioned data testing.
 * 
 * @author K00900207
 * 
 */
public class SingleDummyDataPartitionerImpl implements DataPartitioner
{

	private static final LogService LOGGER = LogServiceFactory.getLogService(SingleDummyDataPartitionerImpl.class.getName());
    public SingleDummyDataPartitionerImpl()
    {
    }

    private List<Partition> allPartitions;

    private Partitioner partitioner;
    /**
     * 
     */
    public void initialize(String basePath, String[] columns, Partitioner partitioner)
    {
    	this.partitioner = partitioner;
//        System.out.println("SingleDummyDataPartitionerImpl initialized");
        LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, "SingleDummyDataPartitionerImpl initialized");

        allPartitions = new ArrayList<Partition>(MolapCommonConstants.CONSTANT_SIZE_TEN);

        PartitionImpl partitionImpl = new PartitionImpl(null, null);

        allPartitions.add(partitionImpl);
    }

    @Override
    public Partition getPartionForTuple(Object[] tuple, long rowCounter)
    {
        return allPartitions.get(0);
    }

    /**
     * 
     */
    public List<Partition> getAllPartitions()
    {
        return allPartitions;
    }

    /**
     * 
     * @see com.huawei.datasight.molap.partition.api.DataPartitioner#getPartitions(com.huawei.datasight.molap.query.MolapQueryPlan)
     * 
     */
    public List<Partition> getPartitions(MolapQueryPlan queryPlan)
    {
        return allPartitions;
    }

    /**
     * Identify the partitions applicable for the given filter
     */
    public List<Partition> getPartitions(Map<String, MolapDimensionLevelFilter> filters)
    {
        return allPartitions;
    }
    
	@Override
	public String[] getPartitionedColumns() 
	{
		return new String[0];
	}

	@Override
	public Partitioner getPartitioner() {
		return partitioner;
	}
}
