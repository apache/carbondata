package com.huawei.unibi.molap.sortandgroupby.sortData;

import java.io.File;

import com.huawei.unibi.molap.sortandgroupby.exception.MolapSortKeyAndGroupByException;

/**
 * Project Name 	: Carbon 
 * Module Name 		: MOLAP Data Processor
 * Author 			: Suprith T 72079 
 * Created Date 	: 24-Aug-2015
 * FileName 		: TempSortFileWriter.java
 * Description 		: Interface for writing the sort temp file
 * Class Version 	: 1.0
 */
public interface TempSortFileWriter
{
    /**
     * Method will be used to initialize
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
