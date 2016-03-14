package com.huawei.datasight.molap.autoagg.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.eigenbase.xom.DOMWrapper;
import org.eigenbase.xom.Parser;
import org.eigenbase.xom.XOMException;
import org.eigenbase.xom.XOMUtil;

import com.google.gson.Gson;
import com.huawei.datasight.molap.core.load.LoadMetadataDetails;
import com.huawei.datasight.molap.datastats.model.LoadModel;
import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.fileperations.AtomicFileOperations;
import com.huawei.unibi.molap.datastorage.store.fileperations.AtomicFileOperationsImpl;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.olap.MolapDef;
import com.huawei.unibi.molap.olap.MolapDef.Schema;
import com.huawei.unibi.molap.util.MolapCoreLogEvent;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * Util class used by other bundle
 * @author A00902717
 *
 */
public final class CommonUtil
{
	private static final String LOAD_NAME = "Load_";
	private static final LogService LOGGER = LogServiceFactory
			.getLogService(CommonUtil.class.getName());
	private CommonUtil()
	{
		// TODO Auto-generated constructor stub
	}
	public static void setListOfValidSlices(String metaPath,LoadModel loadModel)
	{
		String loadMetaPath =metaPath+File.separator + MolapCommonConstants.LOADMETADATA_FILENAME + MolapCommonConstants.MOLAP_METADATA_EXTENSION;
		List<String> listOfValidSlices = new ArrayList<String>(10);
		DataInputStream dataInputStream = null;
		Gson gsonObjectToRead = new Gson();
		
		AtomicFileOperations fileOperation = new AtomicFileOperationsImpl(loadMetaPath, FileFactory.getFileType(loadMetaPath));
		
		try
		{
			if (FileFactory.isFileExist(loadMetaPath,
					FileFactory.getFileType(loadMetaPath)))
			{
			    
			    dataInputStream = fileOperation.openForRead();
			    
				/*dataInputStream = FileFactory.getDataInputStream(
						loadMetaPath,
						FileFactory.getFileType(loadMetaPath));
*/
				BufferedReader buffReader = new BufferedReader(
						new InputStreamReader(dataInputStream, "UTF-8"));

				LoadMetadataDetails[] loadFolderDetailsArray = gsonObjectToRead
						.fromJson(buffReader, LoadMetadataDetails[].class);
				List<String> listOfValidUpdatedSlices = new ArrayList<String>(
						10);
				// just directly iterate Array
				List<LoadMetadataDetails> loadFolderDetails = Arrays
						.asList(loadFolderDetailsArray);
				
				//this will have all load except failed load
				List<String> allLoads=new ArrayList<String>(10);

				for (LoadMetadataDetails loadMetadataDetails : loadFolderDetails)
				{
					if (MolapCommonConstants.STORE_LOADSTATUS_SUCCESS
							.equalsIgnoreCase(loadMetadataDetails
									.getLoadStatus())
							|| MolapCommonConstants.MARKED_FOR_UPDATE
									.equalsIgnoreCase(loadMetadataDetails
											.getLoadStatus())
							|| MolapCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS
									.equalsIgnoreCase(loadMetadataDetails
											.getLoadStatus()))
					{
						if (MolapCommonConstants.MARKED_FOR_UPDATE
								.equalsIgnoreCase(loadMetadataDetails
										.getLoadStatus()))
						{

							listOfValidUpdatedSlices.add(loadMetadataDetails
									.getLoadName());
						}
						listOfValidSlices
								.add(LOAD_NAME+loadMetadataDetails.getLoadName());

					}
					
					if(!MolapCommonConstants.STORE_LOADSTATUS_FAILURE.equals(loadMetadataDetails
							.getLoadStatus()))
					{
						allLoads.add(LOAD_NAME+loadMetadataDetails.getLoadName());
					}
				}
				loadModel.setValidUpdateSlices(listOfValidUpdatedSlices);
				loadModel.setAllLoads(allLoads);
			}

		}
		catch (IOException e)
		{
			LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
					"IO Exception @: " + e.getMessage());
		}

		finally
		{
			MolapUtil.closeStreams(dataInputStream);
		}
		loadModel.setValidSlices(listOfValidSlices);
		
		
	}
	/**
	 * read metadata
	 * @param metaDataPath
	 * @return
	 */
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
		DataInputStream in=null;
		try
		{

			in = FileFactory.getDataInputStream(metaDataPath,
					fileType);
			int len = in.readInt();
			while (len > 0)
			{
				byte[] schemaNameBytes = new byte[len];
				in.readFully(schemaNameBytes);

				//String schemaName = new String(schemaNameBytes, "UTF8");
				int cubeNameLen = in.readInt();
				byte[] cubeNameBytes = new byte[cubeNameLen];
				in.readFully(cubeNameBytes);
				//String cubeName = new String(cubeNameBytes, "UTF8");
				int dataPathLen = in.readInt();
				byte[] dataPathBytes = new byte[dataPathLen];
				in.readFully(dataPathBytes);
				//String dataPath = new String(dataPathBytes, "UTF8");
				
				int versionLength = in.readInt();
				byte[] versionBytes = new byte[versionLength];
				in.readFully(versionBytes);
				//String version = new String(versionBytes,"UTF8");
				
				int schemaLen = in.readInt();
				byte[] schemaBytes = new byte[schemaLen];
				in.readFully(schemaBytes);

				String schema = new String(schemaBytes, "UTF8");
				int partitionLength = in.readInt();
				byte[] partitionBytes = new byte[partitionLength];
				in.readFully(partitionBytes);

				Schema mondSchema = parseStringToSchema(schema);

				list.add(mondSchema);
				try
				{
				    in.readLong();
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
			LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,e.getMessage());
		}
		catch (XOMException e)
		{
			LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,e.getMessage());
		}
		finally
		{
			MolapUtil.closeStreams(in);
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

	public static void fillSchemaAndCubeDetail(LoadModel loadModel)
	{
		String dataStatsPath=loadModel.getMetaDataPath();
		List<Schema> schemas=readMetaData(dataStatsPath+File.separator+"metadata");
		if(null!=schemas)
		{
			Schema schema=schemas.get(0);
			loadModel.setSchema(schema);
			MolapDef.Cube[] cubes = schema.cubes;
			for(MolapDef.Cube cube:cubes)
			{
				if(loadModel.getCubeName().equalsIgnoreCase(cube.name))
				{
					loadModel.setCube(cube);		
				}
			}
			
			
		}
		
	}
	public static boolean isLoadDeleted(List<String> validSlices,
			List<String> calculatedLoads)
	{
		for(String calculatedLoad:calculatedLoads)
		{
			if(!validSlices.contains(calculatedLoad))
			{
				return true;
			}
		}
		return false;
	}

}
