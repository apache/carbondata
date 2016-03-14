package com.huawei.unibi.molap.sortandgroupby.sortData;

import java.io.File;

import com.huawei.unibi.molap.sortandgroupby.exception.MolapSortKeyAndGroupByException;

/**
 * Project Name 	: Carbon 
 * Module Name 		: MOLAP Data Processor
 * Author 			: Suprith T 72079 
 * Created Date 	: 20-Aug-2015
 * FileName 		: SortTempFileChunkWriter.java
 * Description 		: This class will be used to write the compressed sort temp file chunk by chunk
 * Class Version 	: 1.0
 */
public class SortTempFileChunkWriter implements TempSortFileWriter
{
    /**
     * writer
     */
    private TempSortFileWriter writer;
    
    /**
     * recordPerLeaf
     */
    private int recordPerLeaf;
    
    /**
     * MolapCompressedSortTempFileChunkWriter
     * 
     * @param writer
     */
    public SortTempFileChunkWriter(
    		TempSortFileWriter writer, int recordPerLeaf)
    {
        this.writer = writer;
        this.recordPerLeaf = recordPerLeaf;
    }

    /**
     * initialize 
     */
    public void initiaize(File file, int entryCount)
            throws MolapSortKeyAndGroupByException
    {
        this.writer.initiaize(file, entryCount);
    }

    /**
     * finish
     */
    public void finish()
    {
        this.writer.finish();
    }
    
    /**
     * Below method will be used to write the sort temp file chunk by chunk
     */
    public void writeSortTempFile(Object[][] records)
            throws MolapSortKeyAndGroupByException
    {
        int recordCount = 0;
       Object[][] tempRecords;
        while(recordCount < records.length)
        {
            if(records.length-recordCount < recordPerLeaf)
            {
                recordPerLeaf=records.length-recordCount;
            }
            tempRecords = new Object[recordPerLeaf][];
            System.arraycopy(records, recordCount, tempRecords, 0, recordPerLeaf);
            recordCount += recordPerLeaf;
            this.writer.writeSortTempFile(tempRecords);
        }
    }
}
