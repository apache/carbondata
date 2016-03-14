package com.huawei.datasight.molap.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

import org.eigenbase.xom.DOMWrapper;
import org.eigenbase.xom.Parser;
import org.eigenbase.xom.XOMException;
import org.eigenbase.xom.XOMUtil;

import com.huawei.datasight.molap.datastats.model.LoadModel;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.engine.querystats.Preference;
import com.huawei.unibi.molap.metadata.MolapMetadata;
import com.huawei.unibi.molap.olap.MolapDef;
import com.huawei.unibi.molap.olap.MolapDef.Cube;
import com.huawei.unibi.molap.olap.MolapDef.Schema;
import com.huawei.unibi.molap.util.MolapProperties;
import com.huawei.unibi.molap.util.MolapUtil;

public class TestUtil
{

	public static List<Schema> readMetaData(String metaDataPath)
	{
		if (null == metaDataPath) 
		{
			return null;
		}
		FileType fileType = FileFactory.getFileType(metaDataPath);
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
		return new MolapDef.Schema(defin);
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
		StringBuffer dataStatsPath = new StringBuffer(MolapUtil.getCarbonStorePath(schemaName, cubeName)/*MolapProperties
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
