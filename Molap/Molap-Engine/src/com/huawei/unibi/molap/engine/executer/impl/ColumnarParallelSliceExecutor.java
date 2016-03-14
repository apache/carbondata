package com.huawei.unibi.molap.engine.executer.impl;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.DataStoreBlock;
import com.huawei.unibi.molap.engine.executer.SliceExecuter;
import com.huawei.unibi.molap.engine.executer.exception.QueryExecutionException;
import com.huawei.unibi.molap.engine.executer.pagination.impl.QueryResult;
import com.huawei.unibi.molap.engine.executer.processor.ScannedResultProcessor;
import com.huawei.unibi.molap.engine.executer.processor.ScannedResultProcessorImpl;
import com.huawei.unibi.molap.engine.querystats.PartitionDetail;
import com.huawei.unibi.molap.engine.querystats.PartitionStatsCollector;
import com.huawei.unibi.molap.engine.schema.metadata.SliceExecutionInfo;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.iterator.MolapIterator;
import com.huawei.unibi.molap.keygenerator.KeyGenException;
import com.huawei.unibi.molap.util.MolapProperties;

public class ColumnarParallelSliceExecutor implements SliceExecuter
{
    
    /**
     * LOGGER.
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(ParallelSliceExecutorImpl.class.getName());

    /**
     * Executor Service.    
     */
    private ExecutorService execService;
    
    @Override
    public MolapIterator<QueryResult> executeSlices(List<SliceExecutionInfo> infos, int[] sliceIndex) throws QueryExecutionException
    {
        ColumnarSliceExecuter task = null;
        SliceExecutionInfo latestInfo=infos.get(infos.size()-1);
        long startTime = System.currentTimeMillis();
        int numberOfCores=0;
        try
        {
            numberOfCores=Integer.parseInt(MolapProperties.getInstance().getProperty(
                MolapCommonConstants.NUM_CORES, MolapCommonConstants.NUM_CORES_DEFAULT_VAL));
        }
        catch(NumberFormatException e)
        {
            numberOfCores=1;
        }
        
        if(latestInfo.isDetailQuery())
        {
            int numberOfRecordsInMemory = latestInfo.getNumberOfRecordsInMemory();
            numberOfCores=numberOfRecordsInMemory/Integer.parseInt(MolapProperties.getInstance().getProperty(MolapCommonConstants.LEAFNODE_SIZE, MolapCommonConstants.LEAFNODE_SIZE_DEFAULT_VAL));
            if(numberOfCores<1)
            {
                numberOfCores=1;
            }
        }
        execService = Executors.newFixedThreadPool(numberOfCores);
        ScannedResultProcessor scannedResultProcessor = new ScannedResultProcessorImpl(latestInfo);
        try
        {
            for(SliceExecutionInfo info : infos)
            {
                if(!info.isExecutionRequired())
                {
                    continue;
                }
                DataStoreBlock startNode = info.getSlice().getDataCache(info.getTableName())
                        .getDataStoreBlock(info.getKeyGenerator().generateKey(info.getStartKey()), null, true);
                
                DataStoreBlock lastNode = info.getSlice().getDataCache(info.getTableName())
                        .getDataStoreBlock(info.getKeyGenerator().generateKey(info.getEndKey()), null, false);
                long numberOfNodesToScan=lastNode.getNodeNumber()-startNode.getNodeNumber()+1;
                
                //Add this information to QueryDetail
                // queryDetail will be there only when user do <dataframe>.collect
                //TO-DO need to check for all queries
                PartitionStatsCollector partitionStatsCollector=PartitionStatsCollector.getInstance();
                PartitionDetail partitionDetail=partitionStatsCollector.getPartionDetail(info.getQueryId());
                if(null!=partitionDetail)
                {
                    partitionDetail.addNumberOfNodesScanned(numberOfNodesToScan);    
                }
                    task = new ColumnarSliceExecuter(info, scannedResultProcessor, startNode, numberOfNodesToScan);
                    execService.submit(task);
            }
            execService.shutdown();
            execService.awaitTermination(2, TimeUnit.DAYS);
            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "Total time taken for scan " + (System.currentTimeMillis() - startTime));
            return scannedResultProcessor.getQueryResultIterator();
        }
        catch(QueryExecutionException e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, e.getMessage());
            throw new QueryExecutionException(e);
        }
        catch(InterruptedException e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, e.getMessage());
            throw new QueryExecutionException(e);
        }
        catch(KeyGenException e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, e.getMessage());
            throw new QueryExecutionException(e);
        }
        finally
        {
            execService= null;
            latestInfo= null;
            latestInfo = null;
        }
    }
    
}
