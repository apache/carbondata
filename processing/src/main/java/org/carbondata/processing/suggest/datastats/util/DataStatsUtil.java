/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.processing.suggest.datastats.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

import org.carbondata.processing.suggest.autoagg.AutoAggSuggestionFactory;
import org.carbondata.processing.suggest.autoagg.AutoAggSuggestionService;
import org.carbondata.processing.suggest.autoagg.exception.AggSuggestException;
import org.carbondata.processing.suggest.autoagg.model.Request;
import org.carbondata.processing.suggest.datastats.model.DriverDistinctData;
import org.carbondata.processing.suggest.datastats.model.Level;
import org.carbondata.processing.suggest.datastats.model.LoadModel;
import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.MolapFile;
import org.carbondata.core.datastorage.store.filesystem.MolapFileFilter;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.query.datastorage.InMemoryCubeStore;
import org.carbondata.query.executer.QueryExecutor;
import org.carbondata.query.executer.impl.QueryExecutorImpl;
import org.carbondata.query.expression.DataType;
import org.carbondata.query.querystats.Preference;
import org.carbondata.query.util.MolapEngineLogEvent;
import org.carbondata.core.metadata.MolapMetadata.Cube;
import org.carbondata.core.metadata.MolapMetadata.Dimension;
import org.carbondata.core.olap.MolapDef;
import org.carbondata.core.olap.SqlStatement.Type;
import org.carbondata.core.util.MolapProperties;
import org.carbondata.core.util.MolapUtil;

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
