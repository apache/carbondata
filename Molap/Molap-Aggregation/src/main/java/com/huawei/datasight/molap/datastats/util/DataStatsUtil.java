package com.huawei.datasight.molap.datastats.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

import com.huawei.datasight.molap.autoagg.AutoAggSuggestionFactory;
import com.huawei.datasight.molap.autoagg.AutoAggSuggestionService;
import com.huawei.datasight.molap.autoagg.exception.AggSuggestException;
import com.huawei.datasight.molap.autoagg.model.Request;
import com.huawei.datasight.molap.datastats.model.DriverDistinctData;
import com.huawei.datasight.molap.datastats.model.Level;
import com.huawei.datasight.molap.datastats.model.LoadModel;
import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFileFilter;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.engine.datastorage.InMemoryCubeStore;
import com.huawei.unibi.molap.engine.executer.QueryExecutor;
import com.huawei.unibi.molap.engine.executer.impl.QueryExecutorImpl;
import com.huawei.unibi.molap.engine.expression.DataType;
import com.huawei.unibi.molap.engine.querystats.Preference;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.metadata.MolapMetadata.Cube;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.olap.MolapDef;
import com.huawei.unibi.molap.olap.SqlStatement.Type;
import com.huawei.unibi.molap.util.MolapProperties;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * Utility
 * @author A00902717
 *
 */
public final class DataStatsUtil
{
	private static final LogService LOGGER = LogServiceFactory
			.getLogService(DataStatsUtil.class.getName());

	private DataStatsUtil()
	{

	}

	public static MolapFile[] getMolapFactFile(MolapFile file,
			final String table)
	{
		MolapFile[] files = file.listFiles(new MolapFileFilter()
		{
			public boolean accept(MolapFile pathname)
			{
				return (!pathname.isDirectory())
						&& pathname.getName().startsWith(table)
						&& pathname.getName().endsWith(
								MolapCommonConstants.FACT_FILE_EXT);
			}

		});
		return files;
	}

	public static DataType getDataType(Type type)
	{
		switch (type)
		{
		case INT:
			return DataType.IntegerType;
		case DOUBLE:
			return DataType.DoubleType;
		case LONG:
			return DataType.LongType;
		case STRING:
			return DataType.StringType;
		case BOOLEAN:
			return DataType.BooleanType;
		default:
			return DataType.IntegerType;
		}

	}

	/**
	 * Get sample size 
	 * @param dimension
	 * @return
	 */
	public static int getNumberOfRows(Dimension dimension)
	{
		int recCount = 10;
		String conRecCount = MolapProperties.getInstance().getProperty(
				Preference.AGG_REC_COUNT);
		if (null != conRecCount && Integer.parseInt(conRecCount) < recCount)
		{
			recCount = Integer.parseInt(conRecCount);
		}
		return recCount;
	}


	public static boolean createDirectory(String path)
	{
		FileFactory.FileType fileType = FileFactory.getFileType(path);
		try
		{
			if (!FileFactory.isFileExist(path, fileType, false))
			{
				if (!FileFactory.mkdirs(path, fileType))
				{
					return false;
				}
			}
		}
		catch (Exception e)
		{
			return false;
		}
		return true;
	}

	public static void serializeObject(Object object, String path,
			String fileName)
	{
		if (createDirectory(path))
		{
			OutputStream out = null;
			ObjectOutputStream os = null;
			try
			{
				FileType fileType = FileFactory.getFileType(path);
				out = FileFactory.getDataOutputStream(path + File.separator
						+ fileName, fileType);
				os = new ObjectOutputStream(out);
				os.writeObject(object);
			}
			catch (Exception e)
			{
				LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
						"Error in serializing file:" + path + '/' + fileName);
			}

			finally
			{
				MolapUtil.closeStreams(out, os);
			}
		}
	}

	public static Object readSerializedFile(String path)
	{
		Object object = null;
		if (isFileExist(path))
		{
			ObjectInputStream is = null;
			InputStream in = null;

			try
			{
				FileType fileType = FileFactory.getFileType(path);
				in = FileFactory.getDataInputStream(path, fileType);
				is = new ObjectInputStream(in);

				object = is.readObject();

			}
			catch (Exception e)
			{
				LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
						"Error in deserializing file:" + path);
			}

			finally
			{
				MolapUtil.closeStreams(in, is);
			}
		}
		return object;
	}

	private static boolean isFileExist(String path)
	{
		FileFactory.FileType fileType = FileFactory.getFileType(path);

		try
		{
			return FileFactory.isFileExist(path, fileType, false);
		}
		catch (IOException e)
		{
			return false;
		}

	}

	public static QueryExecutor getQueryExecuter(Cube cube, String factTable)
	{
		QueryExecutor executer = new QueryExecutorImpl(
				cube.getDimensions(factTable), cube.getSchemaName(),
				cube.getOnlyCubeName());
		return executer;

	}

	public static void createDataSource(MolapDef.Schema schema,
			Cube cube, String partitionID,
			List<String> sliceLoadPaths, String factTableName, List<String> validUpdateSlices, String dataPath,int restructureNo, long cubeCreationTime)
	{
		InMemoryCubeStore.getInstance().loadCube(schema, cube, partitionID,
				sliceLoadPaths, validUpdateSlices, factTableName,
				dataPath, restructureNo, cubeCreationTime);

	}
	public static Level[] getDistinctDataFromDataStats(LoadModel loadModel) throws AggSuggestException
	{
		StringBuffer dataStatsPath = new StringBuffer(
				loadModel.getMetaDataPath());
		dataStatsPath.append(File.separator).append(
				Preference.AGGREGATE_STORE_DIR);
		// checking for distinct data if its already calculated
		String distinctDataPath = dataStatsPath.toString() + File.separator
				+ Preference.DATASTATS_DISTINCT_FILE_NAME;
		DriverDistinctData driverDistinctData = (DriverDistinctData) DataStatsUtil
				.readSerializedFile(distinctDataPath);
		if (null == driverDistinctData)
		{
			AutoAggSuggestionService dataStatsService = AutoAggSuggestionFactory
					.getAggregateService(Request.DATA_STATS);
			dataStatsService.getAggregateDimensions(loadModel);
			driverDistinctData = (DriverDistinctData) DataStatsUtil
					.readSerializedFile(distinctDataPath);
		}
		
		return driverDistinctData!=null?driverDistinctData.getLevels():null;
	}
	
	/**
	 * @param path
	 * @param folderStsWith
	 * @return
	 */
	public static MolapFile[] getRSFolderListList(LoadModel loadModel)
	{
		String basePath = loadModel.getDataPath();
		basePath = basePath + File.separator + loadModel.getSchemaName()+'_'+loadModel.getPartitionId() + File.separator
				+ loadModel.getCubeName()+'_'+loadModel.getPartitionId();
		MolapFile file = FileFactory.getMolapFile(basePath,
				FileFactory.getFileType(basePath));
		MolapFile[] files = null;
		if (file.isDirectory())
		{
			files = file.listFiles(new MolapFileFilter()
			{

				@Override
				public boolean accept(MolapFile pathname)
				{
					String name = pathname.getName();
					return (pathname.isDirectory())
							&& name.startsWith("RS_");
				}
			});

		}
		return files;
	}
	
    /**
     * 
     * @param queryModel
     * @param listLoadFolders
     * @param cubeUniqueName
     * @return
     * @throws Exception
     * 
     */
	public static List<String> validateAndLoadRequiredSlicesInMemory(
			List<String> listLoadFolders,
			String cubeUniqueName,Set<String> columns) throws AggSuggestException
	{
		try
		{
			List<String> levelCacheKeys = InMemoryCubeStore.getInstance()
	                .loadRequiredLevels(cubeUniqueName, columns, listLoadFolders);
	        return levelCacheKeys;	
		}
		catch(RuntimeException e)
		{
			throw new AggSuggestException("Failed to load level file.",e);
		}
	}

	
}
