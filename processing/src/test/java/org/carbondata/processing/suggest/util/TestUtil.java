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

package org.carbondata.processing.suggest.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.datastorage.store.filesystem.MolapFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.metadata.MolapMetadata;
import org.carbondata.core.olap.MolapDef.*;
import org.carbondata.core.util.MolapUtil;
import org.carbondata.processing.suggest.datastats.model.LoadModel;
import org.carbondata.query.querystats.Preference;
import org.eigenbase.xom.DOMWrapper;
import org.eigenbase.xom.Parser;
import org.eigenbase.xom.XOMException;
import org.eigenbase.xom.XOMUtil;

public class TestUtil
{

	public static List<Schema> readMetaData(String metaDataPath)
	{
		if (null == metaDataPath)
		{
			return null;
		}
		FileFactory.FileType fileType = FileFactory.getFileType(metaDataPath);
		MolapFile metaDataFile = FileFactory.getMolapFile(metaDataPath,
				fileType);
		if (!metaDataFile.exists())
		{
			return null;
		}

		List<Schema> list = new ArrayList<Schema>();
		try
		{

			DataInputStream in = FileFactory.getDataInputStream(metaDataPath,
					fileType);
			int len = in.readInt();
			while (len > 0)
			{
				byte[] schemaNameBytes = new byte[len];
				in.readFully(schemaNameBytes);

				String schemaName = new String(schemaNameBytes, "UTF8");
				int cubeNameLen = in.readInt();
				byte[] cubeNameBytes = new byte[cubeNameLen];
				in.readFully(cubeNameBytes);
				String cubeName = new String(cubeNameBytes, "UTF8");
				int dataPathLen = in.readInt();
				byte[] dataPathBytes = new byte[dataPathLen];
				in.readFully(dataPathBytes);
				String dataPath = new String(dataPathBytes, "UTF8");

				int versionLength = in.readInt();
				byte[] versionBytes = new byte[versionLength];
				in.readFully(versionBytes);
				String version = new String(versionBytes,"UTF8");

				int schemaLen = in.readInt();
				byte[] schemaBytes = new byte[schemaLen];
				in.readFully(schemaBytes);

				String schema = new String(schemaBytes, "UTF8");
				int partitionLength = in.readInt();
				byte[] partitionBytes = new byte[partitionLength];
				in.readFully(partitionBytes);

				ByteArrayInputStream inStream = new ByteArrayInputStream(
						partitionBytes);
				ObjectInputStream objStream = new ObjectInputStream(inStream);

				objStream.close();

				Schema mondSchema = parseStringToSchema(schema);
				MolapMetadata.getInstance().loadSchema(mondSchema);

				list.add(mondSchema);
				try
				{
					len = in.readInt();
				}
				catch (EOFException eof)
				{
					len = 0;
				}

			}

			return list;
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (XOMException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	public static Schema parseStringToSchema(String schema) throws XOMException
	{

		Parser xmlParser = XOMUtil.createDefaultParser();
		ByteArrayInputStream baoi = new ByteArrayInputStream(schema.getBytes());
		DOMWrapper defin = xmlParser.parse(baoi);
		return new Schema(defin);
	}

	public static String getDistinctDataPath(LoadModel loadModel)
	{
		StringBuffer dataStatsPath = new StringBuffer(loadModel.getMetaDataPath());
		dataStatsPath.append(File.separator)
				.append(Preference.AGGREGATE_STORE_DIR)
				.append(File.separator).append(Preference.DATASTATS_DISTINCT_FILE_NAME);
		return dataStatsPath.toString();
	}
	public static String getDataStatsAggCombination(String schemaName,String cubeName,String tableName)
	{
		StringBuffer dataStatsPath = new StringBuffer(
				MolapUtil.getCarbonStorePath(schemaName, cubeName)/*MolapProperties
				.getInstance().getProperty(MolapCommonConstants.STORE_LOCATION)*/);
		dataStatsPath.append(File.separator)
				.append(Preference.AGGREGATE_STORE_DIR)
				.append(File.separator).append(schemaName)
				.append(File.separator).append(cubeName)
				.append(File.separator).append(tableName)
				.append(File.separator).append(Preference.DATA_STATS_FILE_NAME);
		return dataStatsPath.toString();
	}

	public static LoadModel createLoadModel(String schemaName, String cubeName,
			Schema schema, Cube cube ,String dataPath,String metaPath)
	{
		 LoadModel loadModel =new LoadModel();
	     loadModel.setSchema(schema);
		 loadModel.setCube(cube);
		 loadModel.setSchemaName(schemaName);
		 loadModel.setCubeName(cubeName);
		 loadModel.setTableName(cube.fact.getAlias());
		 loadModel.setDataPath(dataPath);
		 loadModel.setMetaDataPath(metaPath);
		 loadModel.setPartitionId("0");
		 loadModel.setMetaCube(null);
		 loadModel.getMetaCube();
		 List<String> validLoad=new ArrayList<String>();
		 validLoad.add("Load_0");
		 loadModel.setValidSlices(validLoad);
		 loadModel.setCubeCreationtime(System.currentTimeMillis());
		return loadModel;
	}

}
