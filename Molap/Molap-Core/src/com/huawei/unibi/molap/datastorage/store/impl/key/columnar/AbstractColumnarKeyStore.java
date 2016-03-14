package com.huawei.unibi.molap.datastorage.store.impl.key.columnar;

import java.util.HashMap;
import java.util.Map;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStore;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreInfo;
import com.huawei.unibi.molap.datastorage.store.compression.Compressor;
import com.huawei.unibi.molap.datastorage.store.compression.SnappyCompression;

public abstract class AbstractColumnarKeyStore implements ColumnarKeyStore
{

    /**
     * compressor will be used to compress the data 
     */
    protected static final Compressor<byte[]> COMPRESSOR = SnappyCompression.SnappyByteCompression.INSTANCE;
    
    protected ColumnarKeyStoreInfo columnarStoreInfo;
    
    protected byte[][] columnarKeyBlockDataIndex;
    
    protected byte[][] columnarKeyBlockData;
    
    protected Map<Integer,Integer> mapOfColumnIndexAndColumnBlockIndex;
    
    protected Map<Integer,Integer> mapOfAggDataIndex;
    
    protected byte[][] columnarUniqueblockKeyBlockIndex;
    
    public AbstractColumnarKeyStore(
            ColumnarKeyStoreInfo columnarStoreInfo, boolean isInMemory,
            FileHolder fileHolder)
    {
        this.columnarStoreInfo=columnarStoreInfo;
        this.mapOfColumnIndexAndColumnBlockIndex = new HashMap<Integer,Integer>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        this.mapOfAggDataIndex = new HashMap<Integer,Integer>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        int index=0;
        for(int i = 0;i < this.columnarStoreInfo.getIsSorted().length;i++)
        {
            if(!this.columnarStoreInfo.getIsSorted()[i])
            {
                this.mapOfColumnIndexAndColumnBlockIndex.put(i,index++);
            }
        }
        index= 0;
        for(int i = 0;i < this.columnarStoreInfo.getAggKeyBlock().length;i++)
        {
            if(this.columnarStoreInfo.getAggKeyBlock()[i])
            {
                mapOfAggDataIndex.put(i,index++);
            }
        }
        if(isInMemory)
        {
            this.columnarKeyBlockData=new byte[this.columnarStoreInfo.getIsSorted().length][];
            this.columnarKeyBlockDataIndex=new byte[this.mapOfColumnIndexAndColumnBlockIndex.size()][];
            this.columnarUniqueblockKeyBlockIndex = new byte[this.mapOfAggDataIndex.size()][];
            for(int i = 0;i < columnarStoreInfo.getSizeOfEachBlock().length;i++)
            {
                columnarKeyBlockData[i] = fileHolder.readByteArray(
                        columnarStoreInfo.getFilePath(),
                        columnarStoreInfo.getKeyBlockOffsets()[i],
                        columnarStoreInfo.getKeyBlockLengths()[i]);
                
                if(!this.columnarStoreInfo.getIsSorted()[i])
                {
                    this.columnarKeyBlockDataIndex[mapOfColumnIndexAndColumnBlockIndex.get(i)]=fileHolder.readByteArray(
                            columnarStoreInfo.getFilePath(),
                            columnarStoreInfo.getKeyBlockIndexOffsets()[mapOfColumnIndexAndColumnBlockIndex.get(i)],
                            columnarStoreInfo.getKeyBlockIndexLength()[mapOfColumnIndexAndColumnBlockIndex.get(i)]);
                }
                
                if(this.columnarStoreInfo.getAggKeyBlock()[i])
                {
                    this.columnarUniqueblockKeyBlockIndex[mapOfAggDataIndex.get(i)]=
                            fileHolder.readByteArray(
                                    columnarStoreInfo
                                            .getFilePath(),
                                    columnarStoreInfo
                                            .getDataIndexMapOffsets()[mapOfAggDataIndex.get(i)],
                                    columnarStoreInfo
                                            .getDataIndexMapLength()[mapOfAggDataIndex.get(i)]);
                }
            }
        }
    }
    
    protected int[] getColumnIndexForNonFilter(int[] columnIndex)
    {
        int[] columnIndexTemp = new int[columnIndex.length];
        
        for(int i = 0;i < columnIndex.length;i++)
        {
            columnIndexTemp[columnIndex[i]]=i;
        }
        return columnIndexTemp;
    }
}
