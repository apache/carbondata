/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
