package com.huawei.unibi.molap.datastorage.store.columnar;

import java.nio.ByteBuffer;



public class ColumnarKeyStoreDataHolder
{
    private byte[] keyblockData;
    
    private ColumnarKeyStoreMetadata columnarKeyStoreMetadata;
    
    public ColumnarKeyStoreDataHolder(final byte[] keyblockData,final ColumnarKeyStoreMetadata columnarKeyStoreMetadata)
    {
        this.keyblockData=keyblockData;
        this.columnarKeyStoreMetadata=columnarKeyStoreMetadata;
    }
    
    public byte[] getKeyBlockData()
    {
        return keyblockData;
    }

    /**
     * @return the columnarKeyStoreMetadata
     */
    public ColumnarKeyStoreMetadata getColumnarKeyStoreMetadata()
    {
        return columnarKeyStoreMetadata;
    }
    
    public void unCompress()
    {
//        if(null==columnarKeyStoreMetadata.getDataIndex() ||columnarKeyStoreMetadata.getDataIndex().length<1)
//        {
//            return;
//        }
        if(columnarKeyStoreMetadata.isUnCompressed())
        {
            return;
        }
        this.keyblockData = UnBlockIndexer.uncompressData(keyblockData,
                columnarKeyStoreMetadata.getDataIndex(),
                columnarKeyStoreMetadata.getEachRowSize());
        columnarKeyStoreMetadata.setUnCompressed(true);
    }
 
    public int getSurrogateKey(int columnIndex)
    {
        byte[] actual = new byte[4];
        int startIndex;
        if(null!=columnarKeyStoreMetadata.getColumnReverseIndex())
        {
            startIndex = columnarKeyStoreMetadata.getColumnReverseIndex()[columnIndex]
                    * columnarKeyStoreMetadata.getEachRowSize();
        }
        else
        {
            startIndex= columnIndex*columnarKeyStoreMetadata.getEachRowSize();
        }
        int destPos = 4 - columnarKeyStoreMetadata.getEachRowSize();
        System.arraycopy(keyblockData, startIndex, actual, destPos, columnarKeyStoreMetadata.getEachRowSize());
        return ByteBuffer.wrap(actual).getInt();
    }
}
