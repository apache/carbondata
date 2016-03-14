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

//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;

//import com.huawei.datasight.molap.partition.api.Partition;
//import com.huawei.unibi.molap.query.metadata.MolapDimensionLevelFilter;

/**
 * 
 * @author K00900207
 *
 */
public class SampleDataPartitionerImplTest
{

    /**
     * 
     * @param args
     * 
     */
   /* public static void main(String[] args)
    {
        List<String> nodes = new ArrayList<String>();
        nodes.add("master");
        nodes.add("slave1");
        nodes.add("slave2");
        nodes.add("slave3");
        
        Collections.sort(nodes);
        
        String baseFilePath = "/test_folder";
        String[] columns = new String[]{"Column1", "Column2", "MSISDN", "Column4"};

        SampleDataPartitionerImpl partitioner = new SampleDataPartitionerImpl();
        Properties properties = new Properties();
        properties.put("partitionCount", "20");
        properties.put("partitionColumn", "MSISDN");
        
        partitioner.initialize(baseFilePath, columns,null);
        
        List<Partition> allPartitions = partitioner.getAllPartitions();
        
        // Distribute partitions to nodes
        DefaultLoadBalancer loadBalancer = new DefaultLoadBalancer(nodes, allPartitions);
        
        for(String nodeName : nodes)
        {
            System.out.println("Partitions distrbuted for node : " + nodeName + " are "+ loadBalancer.getPartitionsForNode(nodeName));
        }
        

        // Identify the partition & Node for the given tuple
        
        List<Object[]> tuples = new ArrayList<Object[]>();
        
        tuples.add(new Object[]{"Column1", "Column2", "1000", "Column4"});
        tuples.add(new Object[]{"Column1", "Column2", "1001", "Column4"});
        tuples.add(new Object[]{"Column1", "Column2", "1002", "Column4"});
        tuples.add(new Object[]{"Column1", "Column2", "1003", "Column4"});
        tuples.add(new Object[]{"Column1", "Column2", "1004", "Column4"});
        tuples.add(new Object[]{"Column1", "Column2", "1005", "Column4"});
        
        int i=0;
        for(Object[] tuple: tuples)
        {
            Partition partitionForTuple = partitioner.getPartionForTuple(tuple, i);
            String node = loadBalancer.getNodeForPartitions(partitionForTuple);
            
            System.out.println(" NodeName is: " + node +  " For Tuple: " + Arrays.toString(tuple) + " And Prtition is: " + partitionForTuple);
            i++;
        }
        
        // Identify the partitions to query based on filters
        MolapDimensionLevelFilter filter = new MolapDimensionLevelFilter();
        Map<String, MolapDimensionLevelFilter> filters = new HashMap<String, MolapDimensionLevelFilter>();
        filters.put("MSISDN", filter);
        
        System.out.println(partitioner.getPartitions(filters).toString());
        
        List<Object> includedMSISDNs = new ArrayList<Object>();
        includedMSISDNs.add("1000");
        includedMSISDNs.add("1002");
        filter.setIncludeFilter(includedMSISDNs);
        
        System.out.println(partitioner.getPartitions(filters).toString());
        
        
        includedMSISDNs = new ArrayList<Object>();
        includedMSISDNs.add("1001");
        includedMSISDNs.add("1007");
        filter.setIncludeFilter(includedMSISDNs);
        System.out.println(partitioner.getPartitions(filters).toString());
    }*/
}
