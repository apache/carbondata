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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.sql.cubemodel.Partitioner;

import com.huawei.datasight.molap.partition.api.DataPartitioner;
import com.huawei.datasight.molap.partition.api.Partition;
import com.huawei.datasight.molap.query.MolapQueryPlan;
import com.huawei.datasight.molap.spark.util.MolapSparkInterFaceLogEvent;
import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
//import com.huawei.unibi.molap.engine.executer.impl.QueryExecutorUtil;
//import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;

/**
 * 
 * @author K00900207
 *
 */
public final class QueryPartitionHelper
{
	  private static final LogService LOGGER = LogServiceFactory.getLogService(QueryPartitionHelper.class.getName());	   
    private static QueryPartitionHelper instance = new QueryPartitionHelper();
    
    public static QueryPartitionHelper getInstance()
    {
        return instance;
    }
    
    private Properties properties;
    private String defaultPartitionerClass;
    
    private Map<String, DataPartitioner> partitionerMap = new HashMap<String, DataPartitioner>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    
    private Map<String, DefaultLoadBalancer> loadBalancerMap = new HashMap<String, DefaultLoadBalancer>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    
    /**
     * Private constructor
     */
    private QueryPartitionHelper()
    {
        
    }
    
    private void checkInitialization(String cubeUniqueName, Partitioner partitioner)
    {
        //Initialise if not done earlier
    	
        //String nodeListString = null;
        if(properties == null)
        {
            properties = loadProperties();
            
           // nodeListString = properties.getProperty("nodeList", "master,slave1,slave2,slave3");
            
            defaultPartitionerClass= properties.getProperty("partitionerClass", "com.huawei.datasight.molap.partition.api.impl.SampleDataPartitionerImpl");
          
            LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, this.getClass().getSimpleName() + " is using following configurations.");
            LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, "partitionerClass : " + defaultPartitionerClass);
          
            LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"nodeList : " + Arrays.toString(partitioner.nodeList()));
//            System.out.println(this.getClass().getSimpleName() + " is using following configurations.");            
//            System.out.println("partitionerClass : " + defaultPartitionerClass);
//            System.out.println("nodeList : " + nodeListString);
        }
        
        if(partitionerMap.get(cubeUniqueName)==null)
        {
            DataPartitioner dataPartitioner;
            try
            {
              /*  if(partitioner == null)
                {
                	String cubePartitionerClass = properties.getProperty("partitionerClass."+cubeUniqueName);
                    if(cubePartitionerClass == null)
                    {
                    	  LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"Using default partitioner class as no specific partitionerClass configured for " + cubeUniqueName);
//                        System.out.println("Using default partitioner class as no specific partitionerClass configured for " + cubeUniqueName);
                        cubePartitionerClass = defaultPartitionerClass;
                    }
                    String[]  nodes	=null;
                    nodes = partitioner.nodeList();
                    Arrays.sort(nodes);
                	partitioner = new Partitioner(cubePartitionerClass,
                			new String[]{properties.getProperty("partitionColumn","")},
                			Integer.parseInt(properties.getProperty("partitionCount", "1")),
                			nodes);
                }*/
                
                dataPartitioner = (DataPartitioner) Class.forName(partitioner.partitionClass()).newInstance();
                dataPartitioner.initialize("", new String[0], partitioner);
                
                List<Partition> partitions = dataPartitioner.getAllPartitions();
                DefaultLoadBalancer loadBalancer = new DefaultLoadBalancer(Arrays.asList(partitioner.nodeList()), partitions);
                partitionerMap.put(cubeUniqueName, dataPartitioner);
                loadBalancerMap.put(cubeUniqueName, loadBalancer);
            }
            catch(ClassNotFoundException e)
            {
//                e.printStackTrace();
            	 LOGGER.error(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, e, e.getMessage());	
            }catch(InstantiationException e){
            	LOGGER.error(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, e, e.getMessage());	
            }catch (IllegalAccessException e) {
            	LOGGER.error(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, e, e.getMessage());
			}
        }
    }
    
    
    /**
     * Get partitions applicable for query based on filters applied in query
     * 
     * @param queryPlan
     * @return
     * 
     */
    public List<Partition> getPartitionsForQuery(MolapQueryPlan queryPlan, Partitioner partitioner)
    {
        String cubeUniqueName = queryPlan.getSchemaName() + '_' + queryPlan.getCubeName();
        checkInitialization(cubeUniqueName, partitioner);
        
        DataPartitioner dataPartitioner = partitionerMap.get(cubeUniqueName);
        
        List<Partition> queryPartitions = dataPartitioner.getPartitions(queryPlan);
        return queryPartitions;
    }
    
    public List<Partition> getAllPartitions(String schemaName, String cubeName, Partitioner partitioner)
    {
        String cubeUniqueName = schemaName + '_' + cubeName;
        checkInitialization(cubeUniqueName, partitioner);
        
        DataPartitioner dataPartitioner = partitionerMap.get(cubeUniqueName);
        
        return dataPartitioner.getAllPartitions();
    }
    
    public void removePartition(String schemaName, String cubeName)
    {
    	String cubeUniqueName = schemaName + '_' + cubeName;
    	partitionerMap.remove(cubeUniqueName);
    }
    
    /**
     * Get the node name where the partition is assigned to.
     * 
     * @param partition
     * @param schemaName
     * @param cubeName
     * @return
     * 
     */
    public String getLocation(Partition partition, String schemaName, String cubeName, Partitioner partitioner)
    {
        String cubeUniqueName = schemaName + '_' + cubeName;
        checkInitialization(cubeUniqueName, partitioner);
        
        DefaultLoadBalancer loadBalancer = loadBalancerMap.get(cubeUniqueName);
        return loadBalancer.getNodeForPartitions(partition);
    }
    
    public String[] getPartitionedColumns(String schemaName, String cubeName, Partitioner partitioner)
    {
        String cubeUniqueName = schemaName + '_' + cubeName;
        checkInitialization(cubeUniqueName, partitioner);
        DataPartitioner dataPartitioner = partitionerMap.get(cubeUniqueName);
        return dataPartitioner.getPartitionedColumns();
    }
    
  /*  public Partitioner getPartitioner(String schemaName, String cubeName)
    {
        String cubeUniqueName = schemaName + '_' + cubeName;
        checkInitialization(cubeUniqueName, null);
        DataPartitioner dataPartitioner = partitionerMap.get(cubeUniqueName);
        return dataPartitioner.getPartitioner();
    }*/
    
//    /**
//     * 
//     * @param partition
//     * @return
//     * 
//     */
//    public String getStorePath(Partition partition)
//    {
//        return properties.getProperty("basePath")+"_"+partition.getUniqueID();
//    }
    
    /**
     * 
     * Read the properties from CSVFilePartitioner.properties
     */
    private static Properties loadProperties()
    {
        Properties properties = new Properties();

        File file = new File("DataPartitioner.properties");
        FileInputStream fis = null;
        try
        {
            if(file.exists())
            {
                fis = new FileInputStream(file);

                properties.load(fis);
            }
        }
        catch(Exception e)
        {
//            e.printStackTrace();
            LOGGER.error(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, e, e.getMessage());	
        }
        finally
        {
            if(null != fis)
            {
                try
                {
                    fis.close();
                }
                catch(IOException e)
                {
//                    e.printStackTrace();
                	 LOGGER.error(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, e, e.getMessage());	
                }
            }
        }
        
        return properties;

    }
    
    /**
     * @param args
     */
  /*  public static void main(String[] args)
    {
        // TODO Auto-generated method stub
    }
*/
}
