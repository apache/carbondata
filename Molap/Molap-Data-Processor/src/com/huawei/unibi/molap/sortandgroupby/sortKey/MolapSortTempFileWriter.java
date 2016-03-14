package com.huawei.unibi.molap.sortandgroupby.sortKey;

import java.io.File;

import com.huawei.unibi.molap.sortandgroupby.exception.MolapSortKeyAndGroupByException;

/**
 * Project Name NSE V3R8C10 
 * Module Name : MOLAP Data Processor
 * Author :k00900841 
 * Created Date:10-Aug-2014
 * FileName : MolapSortTempFileWriter.java
 * Class Description : Interface for writing the sort temp file
 * Class Version 1.0
 */
public interface MolapSortTempFileWriter
{
    /**
     * Method will be used to initialise
     * @param file
     * @param entryCount
     * @throws MolapSortKeyAndGroupByException
     */
    void initiaize(File file,int entryCount) throws MolapSortKeyAndGroupByException;

    /**
     * Method will be used to finish 
     */
    void finish();
    
    /**
     * Below method will be used to write the sort temp file
     * @param records
     * @throws MolapSortKeyAndGroupByException
     */
    void writeSortTempFile(Object[][] records)throws MolapSortKeyAndGroupByException;
}
