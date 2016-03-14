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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.cubemodel.Partitioner;

import com.huawei.datasight.molap.partition.api.DataPartitioner;
import com.huawei.datasight.molap.partition.api.Partition;
import com.huawei.datasight.molap.query.MolapQueryPlan;
import com.huawei.datasight.molap.query.metadata.MolapDimension;
import com.huawei.datasight.molap.query.metadata.MolapDimensionFilter;
import com.huawei.datasight.molap.spark.util.MolapSparkInterFaceLogEvent;
import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.query.metadata.MolapDimensionLevelFilter;

/**
 * 
 * Sample partition based on MSISDN.
 * 
 * @author K00900207
 * 
 */
public class SampleDataPartitionerImpl implements DataPartitioner
{
	private static final LogService LOGGER = LogServiceFactory.getLogService(SampleDataPartitionerImpl.class.getName());
    private int numberOfPartitions=1;
    
    private int partionColumnIndex = -1;
    
    private String partitionColumn; 
    
    private Partitioner partitioner;
    
    public SampleDataPartitionerImpl()
    {
    }
    
    private List<Partition> allPartitions;

    private String baseLocation;

    /**
     * 
     */
    public void initialize(String basePath, String[] columns, Partitioner partitioner)
    {
    	this.partitioner = partitioner;
        numberOfPartitions = partitioner.partitionCount();
        
        partitionColumn = partitioner.partitionColumn()[0];
        LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, "SampleDataPartitionerImpl initializing with following properties.");
        LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, "partitionCount: "+numberOfPartitions);
        LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, "partitionColumn: "+partitionColumn);
        LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, "basePath: "+basePath);
        LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, "columns: "+Arrays.toString(columns));
//        System.out.println("SampleDataPartitionerImpl initializing with following properties.");
//        System.out.println("partitionCount: "+numberOfPartitions);
//        System.out.println("partitionColumn: "+partitionColumn);
//        System.out.println("basePath: "+basePath);
//        System.out.println("columns: "+Arrays.toString(columns));
        
        this.baseLocation = basePath;
        allPartitions = new ArrayList<Partition>(MolapCommonConstants.CONSTANT_SIZE_TEN);

        for(int i = 0;i < columns.length;i++)
        {
            if(columns[i].equalsIgnoreCase(partitionColumn))
            {
                partionColumnIndex = i;
                break;
            }
        }
        
        for(int partionCounter=0; partionCounter< numberOfPartitions; partionCounter++)
        {
            PartitionImpl partitionImpl = new PartitionImpl("" + partionCounter, baseLocation+'/'+partionCounter);
            
            MolapDimensionLevelFilter filter = new MolapDimensionLevelFilter();
            List<Object> includedHashes = new ArrayList<Object>(MolapCommonConstants.CONSTANT_SIZE_TEN);
            includedHashes.add(partionCounter);
            
            filter.setIncludeFilter(includedHashes);
            partitionImpl.setPartitionDetails(partitionColumn, filter);
            
            allPartitions.add(partitionImpl);
        }
    }

    @Override
    public Partition getPartionForTuple(Object[] tuple, long rowCounter)
    {
        int hashCode;
        if(partionColumnIndex==-1)
        {
            hashCode = hashCode(rowCounter);
        }
        else
        {
        	try
        	{
            	//hashCode = hashCode(Long.parseLong((String)tuple[partionColumnIndex]));
        		hashCode = hashCode(((String)tuple[partionColumnIndex]).hashCode());
        	}
        	catch(NumberFormatException e)
        	{
        		hashCode = hashCode(0);
        	}
        }
        return allPartitions.get(hashCode);
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
        MolapDimensionFilter msisdnFilter = null;
        
        Map<MolapDimension, MolapDimensionFilter> filterMap = queryPlan.getDimensionFilters();
        for(Map.Entry<MolapDimension, MolapDimensionFilter> entry: filterMap.entrySet())
        {
            MolapDimension molapDimension = entry.getKey();
            if(partitionColumn.equalsIgnoreCase(molapDimension.getDimensionUniqueName()))
            {
                msisdnFilter = entry.getValue();
                break;
            }
        }
        
        if(msisdnFilter == null || msisdnFilter.getIncludeFilters().size()==0)
        {
           return allPartitions;
        }
        
        
        List<Partition> allowedPartitions = new ArrayList<Partition>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        for(Partition aPartition: allPartitions)
        {
            MolapDimensionLevelFilter partitionFilterDetails = aPartition.getPartitionDetails().get(partitionColumn);
            
            //Check if the partition is serving any of the hash code generated for include filter of query 
            for(String includeFilter: msisdnFilter.getIncludeFilters())
            {
//                try
//                {
                	int hashCode = hashCode(includeFilter.hashCode());
                    if(partitionFilterDetails.getIncludeFilter().contains(hashCode))
                    {
                        allowedPartitions.add(aPartition);
                        break;
                    }
//                }
//                catch(Exception e)
//                {
//                    //Exception means we can't find the partitions due to problem in hascode indetification for filters
//                    return allPartitions;
//                }
               
            }
        }
        
        return allowedPartitions;
    }
    
    /**
     * Identify the partitions applicable for the given filter 
     */
    public List<Partition> getPartitions(Map<String, MolapDimensionLevelFilter> filters)
    {
        if(filters==null || filters.size() == 0 || filters.get(partitionColumn)==null)
        {
            return allPartitions;
        }
        
        
        MolapDimensionLevelFilter msisdnFilter = filters.get(partitionColumn);
        List<Partition> allowedPartitions = new ArrayList<Partition>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        
        if(msisdnFilter.getIncludeFilter().isEmpty())
        {
            // Partition check can be done only for include filter list. If the filter is of other type, 
            //return all the partitions list
            return allPartitions;
        }
        
        for(Partition aPartition: allPartitions)
        {
            MolapDimensionLevelFilter partitionFilterDetails = aPartition.getPartitionDetails().get(partitionColumn);
            
            //Check if the partition is serving any of the hash code generated for include filter of query 
            for(Object includeFilter: msisdnFilter.getIncludeFilter())
            {
//                try
//                {
                	int hashCode = hashCode(((String)includeFilter).hashCode());
                    if(partitionFilterDetails.getIncludeFilter().contains(hashCode))
                    {
                        allowedPartitions.add(aPartition);
                        break;
                    }
//                }
//                catch(Exception e)
//                {
//                    //Exception means we can't find the partitions due to problem in hascode indetification for filters
//                    return allPartitions;
//                }
               
            }
        }
        
        return allowedPartitions;
    }

    private int hashCode(long key) 
    {
        return (int)(Math.abs(key) % numberOfPartitions);
    }

	@Override
	public String[] getPartitionedColumns() 
	{
		return new String[]{partitionColumn};
	}

	@Override
	public Partitioner getPartitioner() {
		return partitioner;
	}

    
}
