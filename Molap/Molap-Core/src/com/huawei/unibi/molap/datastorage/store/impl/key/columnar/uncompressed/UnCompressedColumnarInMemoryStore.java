package com.huawei.unibi.molap.datastorage.store.impl.key.columnar.uncompressed;

import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreInfo;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreMetadata;
import com.huawei.unibi.molap.datastorage.store.impl.key.columnar.AbstractColumnarKeyStore;
import com.huawei.unibi.molap.util.MolapUtil;

public class UnCompressedColumnarInMemoryStore extends AbstractColumnarKeyStore
{

    public UnCompressedColumnarInMemoryStore(
            ColumnarKeyStoreInfo columnarStoreInfo,
            FileHolder fileHolder)
    {
        super(columnarStoreInfo, true, fileHolder);
    }

    @Override
    public ColumnarKeyStoreDataHolder[] getUnCompressedKeyArray(
            FileHolder fileHolder, int[] blockIndex,boolean[] needCompressedData)
    {
        int columnarKeyBlockIndex=0;
        int[] columnIndex= null;
        ColumnarKeyStoreDataHolder [] columnarKeyStoreDataHolders = new ColumnarKeyStoreDataHolder[blockIndex.length];        
        ColumnarKeyStoreMetadata columnarKeyStoreMetadataTemp = null;        
        for(int i = 0;i < columnarKeyStoreDataHolders.length;i++)
        {
            columnarKeyStoreMetadataTemp = new ColumnarKeyStoreMetadata(0);
            if(!columnarStoreInfo.getIsSorted()[blockIndex[i]])
            {
                columnarKeyBlockIndex=mapOfColumnIndexAndColumnBlockIndex.get(blockIndex[i]);
                columnIndex=MolapUtil.getUnCompressColumnIndex(
                        columnarStoreInfo.getKeyBlockIndexLength()[columnarKeyBlockIndex],
                        fileHolder.readByteArray(
                                columnarStoreInfo.getFilePath(),
                                columnarStoreInfo
                                        .getKeyBlockIndexOffsets()[columnarKeyBlockIndex],
                                columnarStoreInfo
                                        .getKeyBlockIndexLength()[columnarKeyBlockIndex]), columnarStoreInfo.getNumberCompressor());
                columnIndex=getColumnIndexForNonFilter(columnIndex);
                columnarKeyStoreMetadataTemp.setColumnIndex(columnIndex);
            }
            columnarKeyStoreMetadataTemp.setSorted(columnarStoreInfo.getIsSorted()[blockIndex[i]]);
            columnarKeyStoreDataHolders[i] = new ColumnarKeyStoreDataHolder(
                    columnarKeyBlockData[blockIndex[i]],columnarKeyStoreMetadataTemp);
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
