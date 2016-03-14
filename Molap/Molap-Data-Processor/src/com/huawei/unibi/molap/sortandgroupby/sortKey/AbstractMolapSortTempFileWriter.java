package com.huawei.unibi.molap.sortandgroupby.sortKey;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import com.huawei.unibi.molap.sortandgroupby.exception.MolapSortKeyAndGroupByException;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * Project Name NSE V3R8C10 
 * Module Name : MOLAP Data Processor
 * Author :k00900841 
 * Created Date:10-Aug-2014
 * FileName : AbstractMolapSortTempFileWriter.java
 * Class Description : abstract class for writing the sort temp file
 * Class Version 1.0
 */
public abstract class AbstractMolapSortTempFileWriter implements MolapSortTempFileWriter
{
    /**
     * Measure count 
     */
    protected int measureCount;

    /**
     * mdkeyIndex
     */
    protected int mdkeyIndex;

    /**
     * isFactMdkeyInSort
     */
    protected boolean isFactMdkeyInSort;

    /**
     * factMdkeyLength
     */
    protected int factMdkeyLength;

    /**
     * writeFileBufferSize
     */
    protected int writeFileBufferSize;

    /**
     * stream
     */
    protected DataOutputStream stream;

    /**
     * mdKeyLength
     */
    protected int mdKeyLength;
    
    /**
     * type
     */
    protected char[] type;
    
    /**
     * AbstractMolapSortTempFileWriter 
     * @param measureCount
     * @param mdkeyIndex
     * @param mdKeyLength
     * @param isFactMdkeyInSort
     * @param factMdkeyLength
     * @param writeFileBufferSize
     */
    public AbstractMolapSortTempFileWriter(
            int measureCount, int mdkeyIndex, int mdKeyLength,
            boolean isFactMdkeyInSort, int factMdkeyLength,
            int writeFileBufferSize, char [] type)
    {
        this.measureCount = measureCount;
        this.mdkeyIndex = mdkeyIndex;
        this.isFactMdkeyInSort = isFactMdkeyInSort;
        this.factMdkeyLength = factMdkeyLength;
        this.writeFileBufferSize = writeFileBufferSize;
        this.mdKeyLength = mdKeyLength;
        this.type = type;
    }
    
    /**
     * Below method will be used to initialise the stream and write the entry count 
     */
    @Override
    public void initiaize(File file, int entryCount)
            throws MolapSortKeyAndGroupByException
    {
        try
        {
            stream = new DataOutputStream(new BufferedOutputStream(
                    new FileOutputStream(file), writeFileBufferSize));
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
	public void finish() {
		MolapUtil.closeStreams(stream);
	}
}
