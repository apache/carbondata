package com.huawei.datasight.molap.datastats.load;

import java.util.ArrayList;
import java.util.List;

import com.huawei.datasight.molap.autoagg.exception.AggSuggestException;
import com.huawei.datasight.molap.datastats.util.DataStatsUtil;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.engine.datastorage.streams.DataInputStream;
import com.huawei.unibi.molap.engine.util.MolapDataInputStreamFactory;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.keygenerator.factory.KeyGeneratorFactory;
import com.huawei.unibi.molap.metadata.MolapMetadata.Cube;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.metadata.SliceMetaData;

/**
 * Each load handler
 * @author A00902717
 *
 */
public class LoadHandler
{


	private Cube metaCube;
	
	private MolapFile loadFolder;
	
	private SliceMetaData sliceMetaData;
	
	private int msrCount;
	
	public LoadHandler(SliceMetaData sliceMetaData,Cube metaCube, MolapFile loadFolder)
	{
		this.metaCube = metaCube;
		this.loadFolder = loadFolder;
		this.sliceMetaData=sliceMetaData;
		if(null!=sliceMetaData.getMeasures())
		{
			msrCount=sliceMetaData.getMeasures().length;
		}
	}

	public FactDataHandler handleFactData(String tableName) throws AggSuggestException
	{
	FactDataHandler factDataHandler=null;
		// Process fact and aggregate data cache
		for (String table : metaCube.getTablesList())
		{
			if (!table.equals(tableName))
			{
				continue;
			}
			factDataHandler=handleTableData(loadFolder, table,msrCount);

		}
		return factDataHandler;
	}
	
	/**
	 * This checks if fact file present. In case of restructure, it will empty load
	 * @param loadFolder
	 * @param table
	 * @return
	 */
	public boolean isDataAvailable(MolapFile loadFolder,String table)
	{
		MolapFile[] files = DataStatsUtil.getMolapFactFile(loadFolder, table);
		if(null== files || files.length==0)
		{
			return false;
		}
		return true;
	}

	private FactDataHandler handleTableData(MolapFile loadFolder, String table,int msrCount) throws AggSuggestException
	{
		// get list of fact file
		MolapFile[] files = DataStatsUtil.getMolapFactFile(loadFolder, table);

		// Create LevelMetaInfo to get each dimension cardinality
		LevelMetaInfo levelMetaInfo = new LevelMetaInfo(loadFolder, table);
		int[] dimensionCardinality = levelMetaInfo.getDimCardinality();
		KeyGenerator keyGenerator = KeyGeneratorFactory
				.getKeyGenerator(dimensionCardinality);
		int keySize = keyGenerator.getKeySizeInBytes();
		String factTableColumn = metaCube.getFactCountColMapping(table);
		boolean hasFactCount = factTableColumn != null
				&& factTableColumn.length() > 0;
		// Create dataInputStream for each fact file
		List<DataInputStream> streams = new ArrayList<DataInputStream>(files.length);
		for (MolapFile aFile : files)
		{

			streams.add(MolapDataInputStreamFactory.getDataInputStream(
					aFile.getAbsolutePath(), keySize, msrCount, hasFactCount,
					loadFolder.getAbsolutePath(), table,
					FileFactory.getFileType()));
		}
		for (DataInputStream stream : streams)
		{
			// Initialize offset value and other detail for each stream
			stream.initInput();
		}

		return new FactDataHandler(metaCube,
				levelMetaInfo, table,keySize,streams);
		

	}
	
	public MolapFile getLoadFolder()
	{
		return loadFolder;
	}

	/**
	 * check if given dimension exist in current load in case of restructure.
	 * @param dimension
	 * @param tableName
	 * @return
	 */
	public boolean isDimensionExist(Dimension dimension,String tableName)
	{
		String[] sliceDimensions=sliceMetaData.getDimensions();
		for(int i=0;i<sliceDimensions.length;i++)
		{
			String colName=sliceDimensions[i].substring(sliceDimensions[i].indexOf(tableName+"_")+tableName.length()+1);
			if(dimension.getColName().equals(colName))
			{
				return true;
			}
		}
		return false;
	}
	

}
