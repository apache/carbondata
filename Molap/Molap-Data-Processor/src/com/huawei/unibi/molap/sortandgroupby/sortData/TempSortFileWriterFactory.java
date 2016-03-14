package com.huawei.unibi.molap.sortandgroupby.sortData;


/**
 * Project Name 	: Carbon 
 * Module Name 		: MOLAP Data Processor
 * Author 			: Suprith T 72079 
 * Created Date 	: 24-Aug-2015
 * FileName 		: TempSortFileWriterFactory.java
 * Description 		: Factory for writing the sort temp file
 * Class Version 	: 1.0
 */
public final class TempSortFileWriterFactory
{
	private static final TempSortFileWriterFactory WRITERFACTORY = new TempSortFileWriterFactory();
	
	private TempSortFileWriterFactory()
	{
		
	}
	
	public static TempSortFileWriterFactory getInstance()
	{
		return WRITERFACTORY;
	}
	
	public TempSortFileWriter getTempSortFileWriter(boolean isCompressionEnabled, int dimensionCount, 
    		int complexDimensionCount, int measureCount,int highCardinalityCount, int writeBufferSize)
	{
		if(isCompressionEnabled)
		{
			return new CompressedTempSortFileWriter(dimensionCount, complexDimensionCount,
					measureCount,highCardinalityCount, writeBufferSize);
		}
		else
		{
			return new UnCompressedTempSortFileWriter(dimensionCount, complexDimensionCount,
					measureCount,highCardinalityCount, writeBufferSize);
		}
	}
}
