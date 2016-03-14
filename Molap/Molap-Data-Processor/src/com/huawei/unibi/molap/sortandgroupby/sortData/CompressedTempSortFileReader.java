package com.huawei.unibi.molap.sortandgroupby.sortData;

import java.io.File;

import com.huawei.unibi.molap.datastorage.store.compression.SnappyCompression.SnappyByteCompression;

/**
 * Project Name 	: Carbon 
 * Module Name 		: MOLAP Data Processor
 * Author 			: Suprith T 72079 
 * Created Date 	: 18-Aug-2015
 * FileName 		: CompressedTempSortFileReader.java
 * Description 		: Class for reading the compressed sort temp file
 * Class Version 	: 1.0
 */
public class CompressedTempSortFileReader extends AbstractTempSortFileReader
{

    /**
     * CompressedTempSortFileReader
     * 
     * @param measureCount
     * @param dimensionCount
     * @param tempFile
     * @param type
     */
    public CompressedTempSortFileReader(int dimensionCount, int complexDimensionCount, int measureCount,
    		File tempFile,int highCardinalityCount)
    {
        super(dimensionCount, complexDimensionCount, measureCount, tempFile, highCardinalityCount);
    }

    /**
     * below method will be used to get chunk of rows
     * 
     * @return row
     */
    @Override
    public Object[][] getRow()
    {
        int recordLength = fileHolder.readInt(filePath);
        int byteArrayLength = fileHolder.readInt(filePath);
        byte[] byteArrayFromFile = SnappyByteCompression.INSTANCE.unCompress(fileHolder.readByteArray(filePath, byteArrayLength));
        return prepareRecordFromByteBuffer(recordLength, byteArrayFromFile);
    }
}
