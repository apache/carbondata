package com.huawei.unibi.molap.sortandgroupby.sortData;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import com.huawei.unibi.molap.sortandgroupby.exception.MolapSortKeyAndGroupByException;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * Project Name 	: Carbon 
 * Module Name 		: MOLAP Data Processor
 * Author 			: Suprith T 72079 
 * Created Date 	: 18-Aug-2015
 * FileName 		: AbstractTempSortFileWriter.java
 * Description 		: Abstract class for writing the sort temp file
 * Class Version 	: 1.0
 */
public abstract class AbstractTempSortFileWriter implements TempSortFileWriter
{

    /**
     * writeFileBufferSize
     */
    protected int writeBufferSize;
    
    /**
     * Measure count 
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
     * stream
     */
    protected DataOutputStream stream;

    /**
     * highCardinalityCount
     */
    protected int highCardinalityCount;
    
    /**
     * AbstractTempSortFileWriter 
     * @param writeBufferSize
     * @param dimensionCount
     * @param measureCount
     */
    public AbstractTempSortFileWriter(int dimensionCount, int complexDimensionCount,
    		int measureCount,int highCardinalityCount, int writeBufferSize)
    {
        this.writeBufferSize = writeBufferSize;
        this.dimensionCount = dimensionCount;
        this.complexDimensionCount = complexDimensionCount;
        this.measureCount = measureCount;
        this.highCardinalityCount = highCardinalityCount;
    }
    
    /**
     * Below method will be used to initialize the stream and write the entry count 
     */
    @Override
    public void initiaize(File file, int entryCount)
            throws MolapSortKeyAndGroupByException
    {
        try
        {
            stream = new DataOutputStream(new BufferedOutputStream(
                    new FileOutputStream(file), writeBufferSize));
            stream.writeInt(entryCount);
        }
        catch(FileNotFoundException e1)
        {
            throw new MolapSortKeyAndGroupByException(e1);
        }
        catch(IOException e)
        {
            throw new MolapSortKeyAndGroupByException(e);
        }
    }

    /**
     * Below method will be used to close the stream
     */
    @Override
    public void finish()
    {
   MolapUtil.closeStreams(stream);
   }
}
