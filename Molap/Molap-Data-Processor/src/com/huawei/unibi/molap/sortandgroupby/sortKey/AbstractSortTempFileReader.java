package com.huawei.unibi.molap.sortandgroupby.sortKey;

import java.io.File;

import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.impl.FileHolderImpl;

public abstract class AbstractSortTempFileReader
{
    /**
     * measure count
     */
    protected int measureCount;

    /**
     * mdKeyLenght
     */
    protected int mdKeyLenght;

    /**
     * isFactMdkeyInSort
     */
    protected boolean isFactMdkeyInSort;

    /**
     * factMdkeyLength
     */
    protected int factMdkeyLength;

    /**
     * entryCount
     */
    protected int entryCount;

    /**
     * fileHolder
     */
    protected FileHolder fileHolder;

    /**
     * filePath
     */
    protected String filePath;

    /**
     * eachRecordSize
     */
    protected int eachRecordSize;

    /**
     * type
     */
    protected char[] type;
    
    /**
     * MolapCompressedSortTempFileReader
     * 
     * @param measureCount
     * @param mdKeyLenght
     * @param isFactMdkeyInSort
     * @param factMdkeyLength
     * @param tempFile
     * @param type
     */
    public AbstractSortTempFileReader(int measureCount, int mdKeyLenght,
            boolean isFactMdkeyInSort, int factMdkeyLength, File tempFile,
            char[] type)
    {
        this.measureCount = measureCount;
        this.mdKeyLenght = mdKeyLenght;
        this.factMdkeyLength = factMdkeyLength;
        this.isFactMdkeyInSort = isFactMdkeyInSort;
        this.fileHolder = new FileHolderImpl(1);
        this.filePath = tempFile.getAbsolutePath();
        entryCount = fileHolder.readInt(filePath);
        eachRecordSize = measureCount + 1;
        this.type = type;
        if(isFactMdkeyInSort)
        {
            eachRecordSize += 1;
        }
    }
    
    /**
     * below method will be used to close the file holder
     */
    public void finish()
    {
        this.fileHolder.finish();
    }
    
    /**
     * Below method will be used to get the total row count in temp file
     * 
     * @return
     */
    public int getEntryCount()
    {
        return entryCount;
    }
    
    /**
     * Below method will be used to get the row
     * @ Author s71955
     * @see com.huawei.unibi.molap.sortandgroupby.sortKey.MolapSortTempFileReader#getRow()
     */
    public abstract Object[][] getRow();
}
