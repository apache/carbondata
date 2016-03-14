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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.huawei.datasight.molap.partition.api.Partition;
import com.huawei.unibi.molap.constants.MolapCommonConstants;

/**
 * A sample load balancer to distribute the partitions to the available nodes in a round robin mode.
 * 
 * @author K00900207
 * 
 */
public class DefaultLoadBalancer
{
//    private List<String> nodeNames;
    
    private Map<String, List<Partition>> nodeToPartitonMap = new HashMap<String, List<Partition>>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    
    private Map<Partition, String> partitonToNodeMap = new HashMap<Partition, String>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    
    public DefaultLoadBalancer(List<String> nodes, List<Partition> partitions)
    {
        //Per form a round robin allocation
        int nodeCount =nodes.size();
        
        int partitioner=0;
        for(Partition partition: partitions)
        {
            int nodeindex = partitioner%nodeCount;
            String node = nodes.get(nodeindex);
            
            List<Partition> oldList = nodeToPartitonMap.get(node);
            if(oldList == null)
            {
                oldList = new ArrayList<Partition>(MolapCommonConstants.CONSTANT_SIZE_TEN);
                nodeToPartitonMap.put(node, oldList);
            }
            oldList.add(partition);
            
            partitonToNodeMap.put(partition, node);
            
            partitioner++;
        }
    }
    
    
    public List<Partition> getPartitionsForNode(String node)
    {
        return  nodeToPartitonMap.get(node);
    }
    
    public String getNodeForPartitions(Partition partition)
    {
        return partitonToNodeMap.get(partition);
    }
}
