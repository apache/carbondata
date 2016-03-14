package com.huawei.unibi.molap.datastorage.store.columnar;

import java.util.Map;

import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.keygenerator.factory.KeyGeneratorFactory;

public class ColumnarKeyStoreMetadata
{
    private boolean isSorted;
    
    private int[] columnIndex;
    
    private int[] columnReverseIndex;
    
    private int eachRowSize;
    
    private int[] dataIndex;
    
    private boolean isUnCompressed;

    private KeyGenerator keyGenerator;

    /**
     * isDirectSurrogateColumn.
     */
    private boolean isDirectSurrogateColumn;



    /**
     * mapOfColumnarKeyBlockData
     */
    private Map<Integer, byte[]> mapOfColumnarKeyBlockData;
    


    public ColumnarKeyStoreMetadata(int eachRowSize)
    {
        this.eachRowSize=eachRowSize;
        keyGenerator = KeyGeneratorFactory.getKeyGenerator(new int[]{eachRowSize});
    }
    
    /**
     * @return the isSorted
     */
    public boolean isSorted()
    {
        return isSorted;
    }

    /**
     * @param isSorted the isSorted to set
     */
    public void setSorted(boolean isSorted)
    {
        this.isSorted = isSorted;
    }

    /**
     * @return the columnIndex
     */
    public int[] getColumnIndex()
    {
        return columnIndex;
    }

    /**
     * @param columnIndex the columnIndex to set
     */
    public void setColumnIndex(int[] columnIndex)
    {
        this.columnIndex = columnIndex;
    }

    /**
     * @return the eachRowSize
     */
    public int getEachRowSize()
    {
        return eachRowSize;
    }

    /**
     * @return the dataIndex
     */
    public int[] getDataIndex()
    {
        return dataIndex;
    }

    /**
     * @param dataIndex the dataIndex to set
     */
    public void setDataIndex(int[] dataIndex)
    {
        this.dataIndex = dataIndex;
    }

    /**
     * @return the columnReverseIndex
     */
    public int[] getColumnReverseIndex()
    {
        return columnReverseIndex;
    }

    /**
     * @param columnReverseIndex the columnReverseIndex to set
     */
    public void setColumnReverseIndex(int[] columnReverseIndex)
    {
        this.columnReverseIndex = columnReverseIndex;
    }

    public boolean isUnCompressed()
    {
        return isUnCompressed;
    }

    public void setUnCompressed(boolean isUnCompressed)
    {
        this.isUnCompressed = isUnCompressed;
    }

    public KeyGenerator getKeyGenerator()
    {
        return keyGenerator;
    }

    /**
     * 
     * @param isDirectSurrogateColumn
     */
    public void setDirectSurrogateColumn(boolean isDirectSurrogateColumn)
    {
       this.isDirectSurrogateColumn=isDirectSurrogateColumn;
        
    }
    /**
     * 
     * @return
     */
    public boolean isDirectSurrogateColumn()
    {
        return isDirectSurrogateColumn;
    }

    /**
     * setDirectSurrogateKeyMembers.
     * @param mapOfColumnarKeyBlockData
     */
    public void setDirectSurrogateKeyMembers(Map<Integer, byte[]> mapOfColumnarKeyBlockData)
    {
        this.mapOfColumnarKeyBlockData=mapOfColumnarKeyBlockData;
        
    }
    
    /**
     * getMapOfColumnarKeyBlockDataForDirectSurroagtes.
     * @return
     */
    public Map<Integer, byte[]> getMapOfColumnarKeyBlockDataForDirectSurroagtes()
    {
        return mapOfColumnarKeyBlockData;
    }
}
