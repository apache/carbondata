package com.huawei.unibi.molap.sortandgroupby.sortData;

import java.io.File;

/**
 * Project Name 	: Carbon 
 * Module Name 		: MOLAP Data Processor
 * Author 			: Suprith T 72079 
 * Created Date 	: 24-Aug-2015
 * FileName 		: TempSortFileReaderFactory.java
 * Description 		: Factory for reading the sort temp file
 * Class Version 	: 1.0
 */
public final class TempSortFileReaderFactory
{
	private static final TempSortFileReaderFactory READERFACTORY = new TempSortFileReaderFactory();
	
	private TempSortFileReaderFactory()
	{
		
	}
	
	public static TempSortFileReaderFactory getInstance()
	{
		return READERFACTORY;
	}
	
	public TempSortFileReader getTempSortFileReader(boolean isCompressionEnabled, int dimensionCount,
			int complexDimensionCount, int measureCount, File tempFile,int highCardinalityCount)
	{
		if(isCompressionEnabled)
		{
			return new CompressedTempSortFileReader(dimensionCount, complexDimensionCount,
					measureCount, tempFile,highCardinalityCount);
		}
		else
		{
			return new UnCompressedTempSortFileReader(dimensionCount, complexDimensionCount,
					measureCount, tempFile,highCardinalityCount);
		}
	}
}
