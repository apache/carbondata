package com.huawei.unibi.molap.store.writer;

import com.huawei.unibi.molap.datastorage.store.columnar.IndexStorage;
import com.huawei.unibi.molap.store.writer.exception.MolapDataWriterException;

public interface MolapFactDataWriter<T> 
 {
	
	
	/**
     * This method will be used to write leaf data to file
     * 
     * file format
     * <key><measure1><measure2>....
	 * @param keyArray
     *          key array
     * @param dataArray
     *          measure array
     * @param entryCount
     *          number of entries
     * @param startKey
     *          start key of leaf
     * @param endKey
     *          end key of leaf
     * @throws MolapDataWriterException 
     *          throws new MolapDataWriterException if any problem
	 */
	
	void writeDataToFile(IndexStorage<T>[] keyStorageArray, byte[][] dataArray,
			int entryCount, byte[] startKey, byte[] endKey)
			throws MolapDataWriterException;
	

	/**
	 * Below method will be used to write the leaf meta data to file 
	 * 
	 * @throws MolapDataWriterException
	 */
	void writeleafMetaDataToFile() throws MolapDataWriterException;

	/**
	 * Below method will be used to initialise the writer 
	 */
	void initializeWriter() throws MolapDataWriterException;

	
	/**
	 * Below method will be used to close the writer 
	 */
	void closeWriter();
	
	/**
	 * Below method will be used to get the leaf meta data size 
	 */
	int getLeafMetadataSize();
	
	/**
	 * For getting TempLocation
	 * @return
	 */
	String getTempStoreLocation();
	
	/**
	 * 
	 * @param isNoDictionary
	 */
	void setIsNoDictionary(boolean[] isNoDictionary);

}
