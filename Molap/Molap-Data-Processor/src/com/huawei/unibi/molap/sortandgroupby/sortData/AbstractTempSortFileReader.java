package com.huawei.unibi.molap.sortandgroupby.sortData;

import java.io.File;
import java.nio.ByteBuffer;

import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.impl.FileHolderImpl;

/**
 * Project Name 	: Carbon 
 * Module Name 		: MOLAP Data Processor
 * Author 			: Suprith T 72079 
 * Created Date 	: 18-Aug-2015
 * FileName 		: AbstractTempSortFileReader.java
 * Description 		: Abstract class for reading the sort temp file
 * Class Version 	: 1.0
 */
public abstract class AbstractTempSortFileReader implements TempSortFileReader
{
    /**
     * measure count
     */
    protected int measureCount;
    
    /**
     * Measure count 
     */
    protected int dimensionCount;
    
    /**
     * complexDimension count 
     */
    protected int complexDimensionCount;

    /**
     * entryCount
     */
    protected int entryCount;

    /**
     * fileHolder
     */
    protected FileHolder fileHolder;

    /**
     * Temp file path
     */
    protected String filePath;

    /**
     * eachRecordSize
     */
    protected int eachRecordSize;
    
    protected int highCardinalityCount;

    /**
     * AbstractTempSortFileReader
     * 
     * @param measureCount
     * @param dimensionCount
     * @param tempFile
     */
    public AbstractTempSortFileReader(int dimensionCount, int complexDimensionCount, int measureCount, File tempFile,int highCardinalityCount)
    {
        this.measureCount = measureCount;
        this.dimensionCount = dimensionCount;
        this.highCardinalityCount = highCardinalityCount;
		this.complexDimensionCount = complexDimensionCount;
        this.fileHolder = new FileHolderImpl(1);
        this.filePath = tempFile.getAbsolutePath();
        entryCount = fileHolder.readInt(filePath);
        eachRecordSize = dimensionCount + complexDimensionCount + measureCount;
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
     * @see com.huawei.unibi.molap.sortandgroupby.sortKey.MolapSortTempFileReader#getRow()
     */
    public abstract Object[][] getRow();

	protected Object[][] prepareRecordFromByteBuffer(int recordLength, byte[] byteArrayFromFile) {
	    Object[][] records = new Object[recordLength][];
	    Object[] record = null;
	    ByteBuffer buffer = ByteBuffer.allocate(byteArrayFromFile.length);
	    
	    buffer.put(byteArrayFromFile);
	    buffer.rewind();
	    
	    int index = 0;
	    byte b = 0;
	    
	    for(int i = 0; i < recordLength; i++)
	    {
	        record = new Object[eachRecordSize];
	        index = 0;
	        
	        for(int j = 0; j < dimensionCount; j++)
	        {
	        	record[index++] = buffer.getInt();
	        }

	        for(int j = 0; j < complexDimensionCount; j++)
	        {
	        	byte[] complexByteArray = new byte[buffer.getInt()];
	        	buffer.get(complexByteArray);
	        	record[index++] = complexByteArray;
	        }
	        
	        for(int j = 0; j < measureCount; j++)
	        {
	        	b = buffer.get();
	            if(b == 1)
	            {
	                record[index++] = buffer.getDouble();
	            }
	            else
	            {
	                record[index++]= null;
	            }
	        }
	        
	        records[i] = record;
	    }
	    return records;
	}
}
