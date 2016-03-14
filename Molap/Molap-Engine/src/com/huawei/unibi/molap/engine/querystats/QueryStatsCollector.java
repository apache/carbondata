package com.huawei.unibi.molap.engine.querystats;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;

/**
 * This class will collect query stats and other details like cube,schema, and
 * fact table and delegate the request to QueryLogger to log the query
 * 
 * @author A00902717
 */
public final class QueryStatsCollector
{

    private static final LogService LOGGER = LogServiceFactory
            .getLogService(QueryStatsCollector.class.getName());

    private static QueryStatsCollector queryStatsCollector;

    private Map<String, QueryDetail> queryStats;
    
    private QueryStore queryStore;
    
    private PartitionAccumulator partitionAccumulator;
    
    private QueryStatsCollector()
    {
        queryStore=new BinaryQueryStore();
        queryStats = new ConcurrentHashMap<String, QueryDetail>(1000);
        partitionAccumulator=new PartitionAccumulator();
    }

    public static synchronized QueryStatsCollector getInstance()
    {
        if(null == queryStatsCollector)
        {
            queryStatsCollector = new QueryStatsCollector();
           
        }
        return queryStatsCollector;
    }

    public void addQueryStats(String queryId, QueryDetail queryDetail)
    {
        queryStats.put(queryId, queryDetail);
    }

    public void removeQueryStats(String queryId)
    {
        queryStats.remove(queryId);
    }
    public QueryDetail getQueryStats(String queryId)
    {
        return queryStats.get(queryId);
    }

    public void logQueryStats(QueryDetail queryDetail)
    {
        try
        {
            if(null!=queryDetail.getDimOrdinals())
            {
                queryStore.logQuery(queryDetail);    
            }    
        }
        catch(Exception e)
        {
            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "Error in logging query:"+e);
        }
        
        
    }
    
    public PartitionDetail getInitialPartitionAccumulatorValue()
    {
        return null;
    }
    
    public PartitionAccumulator getPartitionAccumulatorParam()
    {
        return partitionAccumulator; 
    }
  
}
