package com.huawei.unibi.molap.engine.querystats;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class collect query detail at executor or partition level
 * @author A00902717
 *
 */
public final class PartitionStatsCollector
{
    private static PartitionStatsCollector partitionStatsCollector;

    private Map<String, PartitionDetail> partitionsDetail;

    private PartitionStatsCollector()
    {
        partitionsDetail = new ConcurrentHashMap<String, PartitionDetail>();
    }

    public static synchronized PartitionStatsCollector getInstance()
    {
        if(null == partitionStatsCollector)
        {
            partitionStatsCollector = new PartitionStatsCollector();

        }
        return partitionStatsCollector;
    }

    public void addPartitionDetail(String queryId, PartitionDetail partitionDetail)
    {
        partitionsDetail.put(queryId, partitionDetail);
    }

    public PartitionDetail getPartionDetail(String queryId)
    {
        return partitionsDetail.get(queryId);
    }
    
    public void removePartitionDetail(String queryId)
    {
        partitionsDetail.remove(queryId);
    }

}
