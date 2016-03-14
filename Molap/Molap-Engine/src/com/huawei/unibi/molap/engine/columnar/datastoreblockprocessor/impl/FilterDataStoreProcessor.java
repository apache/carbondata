package com.huawei.unibi.molap.engine.columnar.datastoreblockprocessor.impl;

import java.util.BitSet;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.datastorage.store.dataholder.MolapReadDataHolder;
import com.huawei.unibi.molap.engine.columnar.datastoreblockprocessor.ColumnarDataStoreBlockProcessorInfo;
import com.huawei.unibi.molap.engine.columnar.keyvalue.AbstractColumnarScanResult;
import com.huawei.unibi.molap.engine.columnar.keyvalue.FilterScanResult;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.DataStoreBlock;
import com.huawei.unibi.molap.engine.datastorage.tree.CSBTreeColumnarLeafNode;
import com.huawei.unibi.molap.engine.evaluators.BlockDataHolder;
import com.huawei.unibi.molap.engine.evaluators.FilterEvaluator;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.util.MolapProperties;

public class FilterDataStoreProcessor extends AbstractColumnarDataStoreProcessor
{

    private static final LogService LOGGER = LogServiceFactory.getLogService(FilterDataStoreProcessor.class.getName());
    
    private FilterEvaluator filterEvaluatorTree;

    public FilterDataStoreProcessor(ColumnarDataStoreBlockProcessorInfo columnarDataStoreBlockInfo,
            FilterEvaluator filterEvaluatorTree)
    {
        super(columnarDataStoreBlockInfo);
        this.filterEvaluatorTree = filterEvaluatorTree;
        this.keyValue = new FilterScanResult(this.columnarDataStoreBlockInfo.getKeySize(),columnarDataStoreBlockInfo.getDimensionIndexes());
    }

    public AbstractColumnarScanResult getScannedData(BlockDataHolder blockDataHolder)
    {
        fillKeyValue(blockDataHolder);
        return keyValue;
    }
    
    protected void fillKeyValue(BlockDataHolder blockDataHolder)
    {
        keyValue.reset();
        boolean isMinMaxEnabled=true;
        String minMaxEnableValue=MolapProperties.getInstance().getProperty("molap.enableMinMax");
        if(null!=minMaxEnableValue)
        {
            isMinMaxEnabled=Boolean.parseBoolean(minMaxEnableValue);
        }
        if(isMinMaxEnabled)
        {
            BitSet bitSet = filterEvaluatorTree.isScanRequired(blockDataHolder.getLeafDataBlock().getBlockMaxData(), blockDataHolder.getLeafDataBlock().getBlockMinData());
            
            if(bitSet.isEmpty())
            {
                keyValue.setNumberOfRows(0);
                keyValue.setIndexes(new int[0]);
                DataStoreBlock dataStoreBlock=blockDataHolder.getLeafDataBlock();
                if(dataStoreBlock instanceof CSBTreeColumnarLeafNode)
                {
                    String factFile = ((CSBTreeColumnarLeafNode)dataStoreBlock).getFactFile();
                    LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Skipping fact file because it is not required to scan based on filter condtion:"+factFile);
                }
                return;  
            }    
        }
        
        
        BitSet bitSet = filterEvaluatorTree.applyFilter(blockDataHolder, null);
        
        if(bitSet.isEmpty())
        {
            keyValue.setNumberOfRows(0);
            keyValue.setIndexes(new int[0]);
            return;
        }
        int[] indexes = new int[bitSet.cardinality()];
        int index = 0;
        for(int i = bitSet.nextSetBit(0);i >= 0;i = bitSet.nextSetBit(i + 1))
        {
            indexes[index++] = i;
        }

        ColumnarKeyStoreDataHolder[] keyBlocks = new ColumnarKeyStoreDataHolder[columnarDataStoreBlockInfo
                .getAllSelectedDimensions().length];

        for(int i = 0;i < columnarDataStoreBlockInfo.getAllSelectedDimensions().length;i++)
        {
            if(null == blockDataHolder.getColumnarKeyStore()[columnarDataStoreBlockInfo.getAllSelectedDimensions()[i]])
            {
                keyBlocks[i] = blockDataHolder.getLeafDataBlock().getColumnarKeyStore(
                        columnarDataStoreBlockInfo.getFileHolder(),
                        columnarDataStoreBlockInfo.getAllSelectedDimensions()[i], false);
            }
            else
            {      //CHECKSTYLE:OFF    Approval No:Approval-V1R2C10_001
                keyBlocks[i] = blockDataHolder.getColumnarKeyStore()[columnarDataStoreBlockInfo
                        .getAllSelectedDimensions()[i]];
//                if(columnarDataStoreBlockInfo.getIsUniqueBlock()[columnarDataStoreBlockInfo.getAllSelectedDimensions()[i]])
//                {
                if(null!=keyBlocks[i].getColumnarKeyStoreMetadata().getDataIndex())
                {
                    keyBlocks[i].unCompress();
                }
//                }
              //CHECKSTYLE:ON
            }
        }
        
        ColumnarKeyStoreDataHolder[] temp = new  ColumnarKeyStoreDataHolder[columnarDataStoreBlockInfo.getTotalNumberOfDimension()];
        for(int i = 0;i < columnarDataStoreBlockInfo.getAllSelectedDimensions().length;i++)
        {
            temp[columnarDataStoreBlockInfo.getAllSelectedDimensions()[i]]=keyBlocks[i];
        }
        
        MolapReadDataHolder[]  msrBlocks = new MolapReadDataHolder[columnarDataStoreBlockInfo.getTotalNumberOfMeasures()];
        for(int i = 0;i < columnarDataStoreBlockInfo.getAllSelectedMeasures().length;i++)
        {
            if(null==blockDataHolder.getMeasureBlocks()[columnarDataStoreBlockInfo.getAllSelectedMeasures()[i]])
            {      //CHECKSTYLE:OFF    Approval No:Approval-V1R2C10_001
                msrBlocks[columnarDataStoreBlockInfo.getAllSelectedMeasures()[i]] = blockDataHolder
                        .getLeafDataBlock()
                        .getNodeMsrDataWrapper(columnarDataStoreBlockInfo.getAllSelectedMeasures()[i],
                                columnarDataStoreBlockInfo.getFileHolder()).getValues()[columnarDataStoreBlockInfo.getAllSelectedMeasures()[i]];
            }
            else
            {
                msrBlocks[columnarDataStoreBlockInfo.getAllSelectedMeasures()[i]] = blockDataHolder.getMeasureBlocks()[columnarDataStoreBlockInfo.getAllSelectedMeasures()[i]];
            }//CHECKSTYLE:ON
        }
        keyValue.setMeasureBlock(msrBlocks);
        keyValue.setNumberOfRows(indexes.length);
        keyValue.setKeyBlock(temp);
        keyValue.setIndexes(indexes);
    }

}
