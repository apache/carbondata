package com.huawei.unibi.molap.sortandgroupby.sortKey;

import java.io.File;

import com.huawei.unibi.molap.sortandgroupby.exception.MolapSortKeyAndGroupByException;

/**
 * Project Name NSE V3R8C10 
 * Module Name : MOLAP Data Processor
 * Author :k00900841 
 * Created Date:10-Aug-2014
 * FileName : MolapCompressedSortTempFileChunkWriter.java
 * Class Description : Below class will be used to write the compressed sort temp file chunk by chunk
 * Class Version 1.0
 */
public class MolapSortTempFileChunkWriter implements MolapSortTempFileWriter
{
    /**
     * writer
     */
    private MolapSortTempFileWriter writer;
    
    /**
     * recordPerLeaf
     */
    private int recordPerLeaf;
    
    /**
     * MolapCompressedSortTempFileChunkWriter
     * 
     * @param writer
     */
    public MolapSortTempFileChunkWriter(
            MolapSortTempFileWriter writer, int recordPerLeaf)
    {
        this.writer = writer;
        this.recordPerLeaf=recordPerLeaf;
    }

   
    /**
     * finish
     */
    public void finish()
    {
        this.writer.finish();
    }
   
    /**
     * initialise 
     */
    public void initiaize(File file,int entryCount)
            throws MolapSortKeyAndGroupByException
    {
        this.writer.initiaize(file,entryCount);
    }
    
    /**
     * Below method will be used to write the sort temp file chunk by chunk
     */
    public void writeSortTempFile(Object[][] records)
            throws MolapSortKeyAndGroupByException
    {
        int recordCount = 0;
        Object[][] tempRecords = null;
        while(recordCount<records.length)
        {
            if(records.length-recordCount<recordPerLeaf)
            {
                recordPerLeaf=records.length-recordCount;
            }
            tempRecords = new Object[recordPerLeaf][];
            System.arraycopy(records, recordCount, tempRecords, 0, recordPerLeaf);
            recordCount+=recordPerLeaf;
            this.writer.writeSortTempFile(tempRecords);
        }
        
    }
}
