package com.huawei.unibi.molap.datastorage.store.impl.key.columnar.uncompressed;

import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreInfo;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreMetadata;
import com.huawei.unibi.molap.datastorage.store.columnar.UnBlockIndexer;
import com.huawei.unibi.molap.datastorage.store.impl.key.columnar.AbstractColumnarKeyStore;
import com.huawei.unibi.molap.util.MolapUtil;

public class UnCompressedColumnarFileKeyStore extends AbstractColumnarKeyStore
{

    public UnCompressedColumnarFileKeyStore(ColumnarKeyStoreInfo columnarStoreInfo)
    {
        super(columnarStoreInfo, false, null);
    }

    @Override
    public ColumnarKeyStoreDataHolder[] getUnCompressedKeyArray(
            FileHolder fileHolder, int[] blockIndex,boolean[] needCompressedData)
    {
        ColumnarKeyStoreDataHolder [] columnarKeyStoreDataHolders = new ColumnarKeyStoreDataHolder[blockIndex.length];
        byte[] columnarKeyBlockData = null;
        int[] columnKeyBlockIndex=null;
        ColumnarKeyStoreMetadata columnarKeyStoreMetadata = null;
        int columnarKeyBlockIndex=0;
        int[] dataIndex= null;
        int[] columnKeyBlockReverseIndex=null;
        for(int j = 0;j < columnarKeyStoreDataHolders.length;j++)
        {
            columnarKeyBlockData = fileHolder.readByteArray(
                    columnarStoreInfo.getFilePath(),
                    columnarStoreInfo.getKeyBlockOffsets()[blockIndex[j]],
                    columnarStoreInfo.getKeyBlockLengths()[blockIndex[j]]);
            if(this.columnarStoreInfo.getAggKeyBlock()[blockIndex[j]])
            {
                dataIndex= columnarStoreInfo
                .getNumberCompressor()
                .unCompress(
                        fileHolder.readByteArray(
                                columnarStoreInfo
                                        .getFilePath(),
                                columnarStoreInfo
                                        .getDataIndexMapOffsets()[mapOfAggDataIndex.get(blockIndex[j])],
                                columnarStoreInfo
                                        .getDataIndexMapLength()[mapOfAggDataIndex.get(blockIndex[j])]));
                if(!needCompressedData[j])
                {
                    columnarKeyBlockData = UnBlockIndexer
                            .uncompressData(
                                    columnarKeyBlockData,
                                    dataIndex,
                                    columnarStoreInfo.getSizeOfEachBlock()[blockIndex[j]]);
                    dataIndex = null;
                }
            }
            if(!columnarStoreInfo.getIsSorted()[blockIndex[j]])
            {
                columnarKeyBlockIndex=mapOfColumnIndexAndColumnBlockIndex.get(blockIndex[j]);
                columnKeyBlockIndex=MolapUtil.getUnCompressColumnIndex(
                        columnarStoreInfo.getKeyBlockIndexLength()[columnarKeyBlockIndex],
                        fileHolder.readByteArray(
                                columnarStoreInfo.getFilePath(),
                                columnarStoreInfo
                                        .getKeyBlockIndexOffsets()[columnarKeyBlockIndex],
                                columnarStoreInfo
                                        .getKeyBlockIndexLength()[columnarKeyBlockIndex]), columnarStoreInfo.getNumberCompressor());
                columnKeyBlockReverseIndex=getColumnIndexForNonFilter(columnKeyBlockIndex);
            }
            columnarKeyStoreMetadata = new ColumnarKeyStoreMetadata(columnarStoreInfo.getSizeOfEachBlock()[blockIndex[j]]);
            columnarKeyStoreMetadata.setSorted(columnarStoreInfo.getIsSorted()[blockIndex[j]]);
            columnarKeyStoreMetadata.setColumnIndex(columnKeyBlockIndex);
            columnarKeyStoreMetadata.setDataIndex(dataIndex);
            columnarKeyStoreMetadata.setColumnReverseIndex(columnKeyBlockReverseIndex);
            columnarKeyStoreDataHolders[j] = new ColumnarKeyStoreDataHolder(
                    columnarKeyBlockData,columnarKeyStoreMetadata);
        }
        return columnarKeyStoreDataHolders;
    }

    @Override
    public ColumnarKeyStoreDataHolder getUnCompressedKeyArray(FileHolder fileHolder, int blockIndex,
            boolean needCompressedData)
    {
        // TODO Auto-generated method stub
        return null;
    }
}
