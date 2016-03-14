package com.huawei.unibi.molap.datastorage.store.impl.key.columnar.compressed;

import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreInfo;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreMetadata;
import com.huawei.unibi.molap.datastorage.store.columnar.UnBlockIndexer;
import com.huawei.unibi.molap.datastorage.store.impl.key.columnar.AbstractColumnarKeyStore;
import com.huawei.unibi.molap.util.MolapUtil;

public class CompressedColumnarInMemoryStore extends AbstractColumnarKeyStore
{

    public CompressedColumnarInMemoryStore(
            ColumnarKeyStoreInfo columnarStoreInfo,
            FileHolder fileHolder)
    {
        super(columnarStoreInfo, true, fileHolder);
    }

    @Override
    public ColumnarKeyStoreDataHolder[] getUnCompressedKeyArray(
            FileHolder fileHolder, int[] blockIndex, boolean[] needCompressedData)
    {
        ColumnarKeyStoreDataHolder [] columnarKeyStoreDataHolders = new ColumnarKeyStoreDataHolder[blockIndex.length];
        for(int i = 0;i < columnarKeyStoreDataHolders.length;i++)
        {
            byte[] columnarKeyBlockDataTemp = null;
			int[] columnKeyBlockIndex=null;
			int[] columnKeyBlockReverseIndexes=null;
			ColumnarKeyStoreMetadata columnarKeyStoreMetadata = null;
			int columnarKeyBlockIndex=0;
			int[] dataIndex= null;
			boolean isUnCompressed=true;
            columnarKeyBlockDataTemp = COMPRESSOR.unCompress(columnarKeyBlockData[blockIndex[i]]);
            if(this.columnarStoreInfo.getAggKeyBlock()[blockIndex[i]])
            {
                dataIndex= columnarStoreInfo
                        .getNumberCompressor()
                        .unCompress(columnarUniqueblockKeyBlockIndex[mapOfAggDataIndex.get(blockIndex[i])]);
                if(!needCompressedData[i])
                {
                    columnarKeyBlockDataTemp = UnBlockIndexer
                            .uncompressData(
                                    columnarKeyBlockDataTemp,
                                    dataIndex,
                                    columnarStoreInfo.getSizeOfEachBlock()[blockIndex[i]]);
                    dataIndex = null;
                }
                else
                {
                    isUnCompressed=false;
                }
            }
            if(!columnarStoreInfo.getIsSorted()[blockIndex[i]])
            {
                columnarKeyBlockIndex=mapOfColumnIndexAndColumnBlockIndex.get(blockIndex[i]);
                columnKeyBlockIndex=MolapUtil.getUnCompressColumnIndex(
                        columnarStoreInfo.getKeyBlockIndexLength()[columnarKeyBlockIndex],
                        columnarKeyBlockDataIndex[columnarKeyBlockIndex], columnarStoreInfo.getNumberCompressor());
                columnKeyBlockReverseIndexes=getColumnIndexForNonFilter(columnKeyBlockIndex);
            }
            columnarKeyStoreMetadata = new ColumnarKeyStoreMetadata(columnarStoreInfo.getSizeOfEachBlock()[blockIndex[i]]);
            columnarKeyStoreMetadata.setColumnIndex(columnKeyBlockIndex);
            columnarKeyStoreMetadata.setSorted(columnarStoreInfo.getIsSorted()[blockIndex[i]]);
            columnarKeyStoreMetadata.setDataIndex(dataIndex);
            columnarKeyStoreMetadata.setColumnReverseIndex(columnKeyBlockReverseIndexes);
            columnarKeyStoreMetadata.setUnCompressed(isUnCompressed);
            columnarKeyStoreDataHolders[i] = new ColumnarKeyStoreDataHolder(
                    columnarKeyBlockDataTemp,columnarKeyStoreMetadata);
        }
        return columnarKeyStoreDataHolders;
    }

    @Override
    public ColumnarKeyStoreDataHolder getUnCompressedKeyArray(FileHolder fileHolder, int blockIndex,
            boolean needCompressedData)
    {

        byte[] columnarKeyBlockDataTemp = null;
        int[] columnKeyBlockIndex = null;
        int[] columnKeyBlockReverseIndex = null;
        ColumnarKeyStoreMetadata columnarKeyStoreMetadata = null;
        int columnarKeyBlockIndex = 0;
        int[] dataIndex = null;
        boolean isUnCompressed=true;
        columnarKeyBlockDataTemp =  COMPRESSOR.unCompress(columnarKeyBlockData[blockIndex]);
        if(this.columnarStoreInfo.getAggKeyBlock()[blockIndex])
        {
            dataIndex= columnarStoreInfo
                    .getNumberCompressor()
                    .unCompress(columnarUniqueblockKeyBlockIndex[mapOfAggDataIndex.get(blockIndex)]);
            if(!needCompressedData)
            {
                columnarKeyBlockDataTemp = UnBlockIndexer.uncompressData(columnarKeyBlockDataTemp, dataIndex,
                        columnarStoreInfo.getSizeOfEachBlock()[blockIndex]);
                dataIndex = null;
            }
            else
            {
                isUnCompressed=false;
            }
        }
        if(!columnarStoreInfo.getIsSorted()[blockIndex])
        {
            columnarKeyBlockIndex = mapOfColumnIndexAndColumnBlockIndex.get(blockIndex);
            columnKeyBlockIndex=MolapUtil.getUnCompressColumnIndex(
                    columnarStoreInfo.getKeyBlockIndexLength()[columnarKeyBlockIndex],
                    columnarKeyBlockDataIndex[columnarKeyBlockIndex], columnarStoreInfo.getNumberCompressor());
            columnKeyBlockReverseIndex = getColumnIndexForNonFilter(columnKeyBlockIndex);
        }
        columnarKeyStoreMetadata = new ColumnarKeyStoreMetadata(columnarStoreInfo.getSizeOfEachBlock()[blockIndex]);
        columnarKeyStoreMetadata.setColumnIndex(columnKeyBlockIndex);
        columnarKeyStoreMetadata.setSorted(columnarStoreInfo.getIsSorted()[blockIndex]);
        columnarKeyStoreMetadata.setDataIndex(dataIndex);
        columnarKeyStoreMetadata.setColumnReverseIndex(columnKeyBlockReverseIndex);
        columnarKeyStoreMetadata.setUnCompressed(isUnCompressed);
        ColumnarKeyStoreDataHolder columnarKeyStoreDataHolders = new ColumnarKeyStoreDataHolder(columnarKeyBlockDataTemp,
                columnarKeyStoreMetadata);
        return columnarKeyStoreDataHolders;
    
    }

}
