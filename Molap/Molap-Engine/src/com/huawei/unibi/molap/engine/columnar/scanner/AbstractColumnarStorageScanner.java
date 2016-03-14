package com.huawei.unibi.molap.engine.columnar.scanner;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.engine.columnar.aggregator.ColumnarScannedResultAggregator;
import com.huawei.unibi.molap.engine.columnar.datastoreblockprocessor.DataStoreBlockProcessor;
import com.huawei.unibi.molap.engine.columnar.scanner.impl.BtreeLeafNodeIterator;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.DataStoreBlock;
import com.huawei.unibi.molap.engine.datastorage.tree.CSBTreeColumnarLeafNode;
import com.huawei.unibi.molap.engine.evaluators.BlockDataHolder;
import com.huawei.unibi.molap.engine.executer.impl.RestructureHolder;
import com.huawei.unibi.molap.engine.executer.processor.ScannedResultProcessor;
import com.huawei.unibi.molap.engine.querystats.PartitionDetail;
import com.huawei.unibi.molap.engine.querystats.PartitionStatsCollector;
import com.huawei.unibi.molap.engine.schema.metadata.ColumnarStorageScannerInfo;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.iterator.MolapIterator;

public abstract class AbstractColumnarStorageScanner implements ColumnarStorageScanner
{
    
    /**
     * LOGGER.
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(AbstractColumnarStorageScanner.class.getName());
    
    protected MolapIterator<DataStoreBlock> leafIterator;
    
    protected DataStoreBlockProcessor blockProcessor;
    
    protected ScannedResultProcessor scannedResultProcessor;
    
    protected RestructureHolder restructurHolder;
    
    protected ColumnarScannedResultAggregator columnarAggaregator;
    
    protected BlockDataHolder blockDataHolder;
    
    private String queryId;
    
    public AbstractColumnarStorageScanner(ColumnarStorageScannerInfo columnarStorageScannerInfo)
    {
        leafIterator = new BtreeLeafNodeIterator(columnarStorageScannerInfo.getDatablock(),
                columnarStorageScannerInfo.getTotalNumberOfBlocksToScan());
        this.queryId=columnarStorageScannerInfo.getQueryId();
       
        this.blockProcessor = columnarStorageScannerInfo.getBlockProcessor();
        this.scannedResultProcessor=columnarStorageScannerInfo.getScannedResultProcessor();
        this.restructurHolder = columnarStorageScannerInfo.getRestructurHolder();
        this.blockDataHolder = new BlockDataHolder(columnarStorageScannerInfo.getDimColumnCount(), columnarStorageScannerInfo.getMsrColumnCount());
        this.blockDataHolder.setFileHolder(columnarStorageScannerInfo.getFileHolder());
    }
    
    protected void finish()
    {
        try
        {
            this.scannedResultProcessor.addScannedResult(columnarAggaregator.getResult(restructurHolder));
        }
        catch(Exception e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
        }
    }
    /**
     * Add Query statistics, so that it can be logged analyzed for aggregate table suggestion
     * @param blockDataHolder
     */
    public void addToQueryStats(BlockDataHolder blockDataHolder)
    {
        PartitionStatsCollector partitionStatsCollector=PartitionStatsCollector.getInstance();
        PartitionDetail partitionDetail=partitionStatsCollector.getPartionDetail(queryId);
        if(null==partitionDetail)
        {
            return;
        }
        DataStoreBlock dataStoreBlock = blockDataHolder.getLeafDataBlock();
        if(dataStoreBlock instanceof CSBTreeColumnarLeafNode)
        {
            partitionDetail.addNumberOfRowsScanned(blockDataHolder.getLeafDataBlock().getnKeys());
            partitionStatsCollector.addPartitionDetail(queryId, partitionDetail);
        }

    }
    
}
