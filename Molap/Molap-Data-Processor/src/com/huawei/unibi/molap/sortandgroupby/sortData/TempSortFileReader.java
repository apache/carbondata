package com.huawei.unibi.molap.sortandgroupby.sortData;

/**
 * Project Name 	: Carbon 
 * Module Name 		: MOLAP Data Processor
 * Author 			: Suprith T 72079 
 * Created Date 	: 24-Aug-2015
 * FileName 		: TempSortFileReader.java
 * Description 		: Interface for reading the sort temp file
 * Class Version 	: 1.0
 */
public interface TempSortFileReader
{
	/**
     * below method will be used to close the file holder
     */
    void finish();
    
    /**
     * Below method will be used to get the row
     */
    Object[][] getRow();
    
    /**
     * Below method will be used to get the total row count in temp file
     * 
     * @return
     */
    int getEntryCount();
}
