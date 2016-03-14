/**
 * Project Name  : Carbon 
 * Module Name   : MOLAP Data Processor
 * Author    : R00903928
 * Created Date  : 15-Sep-2015
 * FileName   : LoadMetadataUtil.java
 * Description   : Kettle step to generate MD Key
 * Class Version  : 1.0
 */
package com.huawei.datasight.molap.spark.util;

import java.io.File;

import com.huawei.datasight.molap.core.load.LoadMetadataDetails;
import com.huawei.datasight.molap.load.MolapLoadModel;
import com.huawei.datasight.molap.load.MolapLoaderUtil;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFileFilter;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * Project Name  : Carbon 
 * Module Name   : MOLAP spark interface
 * Author    : R00903928
 * Created Date  : 15-Sep-2015
 * FileName   : LoadMetadataUtil.java
 * Description   : 
 * Class Version  : 1.0
 */
public final class LoadMetadataUtil 
{
	
//	public static LoadMetadataDetails[] readLoadMetadata(String cubeFolderPath)
// {
//
//		Gson gsonObjectToRead = new Gson();
//		DataInputStream dataInputStream = null;
//		BufferedReader buffReader = null;
//		InputStreamReader inStream = null;
//		String metadataFileName = cubeFolderPath+MolapCommonConstants.FILE_SEPARATOR+MolapCommonConstants.LOADMETADATA_FILENAME+MolapCommonConstants.MOLAP_METADATA_EXTENSION;
//		LoadMetadataDetails[] listOfLoadFolderDetailsArray;
//		
//		try 
//		{
//			if(!FileFactory.isFileExist(metadataFileName, FileFactory.getFileType(metadataFileName)))
//			{
//				return new LoadMetadataDetails[0];
//			}
//			dataInputStream = FileFactory.getDataInputStream(metadataFileName,
//					FileFactory.getFileType(metadataFileName));
//			inStream = new InputStreamReader(
//					dataInputStream,
//					MolapCommonConstants.MOLAP_DEFAULT_STREAM_ENCODEFORMAT);
//			buffReader = new BufferedReader(
//					inStream);
//			listOfLoadFolderDetailsArray = gsonObjectToRead
//					.fromJson(buffReader, LoadMetadataDetails[].class);
//		}
//		catch (IOException e) 
//		{
//		    return new LoadMetadataDetails[0];
//		}
//		finally
//		{
//				MolapUtil.closeStreams(buffReader,inStream,dataInputStream);
//		}
//
//
//		return listOfLoadFolderDetailsArray;
//	}
	private LoadMetadataUtil()
	{
		
	}
	/**
	 * 
	 * @param loadModel
	 * @return
	 */
	 public static boolean isLoadDeletionRequired(MolapLoadModel loadModel)
	    {
	       String  metaDataLocation =  MolapLoaderUtil.extractLoadMetadataFileLocation(loadModel.getSchema(), 
	    		   loadModel.getSchemaName(), 
	    		   loadModel.getCubeName());
	       LoadMetadataDetails [] details = MolapUtil.readLoadMetadata(metaDataLocation);
	       if(details!=null&&details.length != 0)
	        {
	            for(LoadMetadataDetails oneRow : details)
	            {
	                if(MolapCommonConstants.MARKED_FOR_DELETE.equalsIgnoreCase(oneRow.getLoadStatus()) && 
	                        oneRow.getVisibility().equalsIgnoreCase("true"))
	                {
	                    return true;
	                }
	            }
	        }
	        
	        return false;
	        
	    }
	    
	 /**
	  * 
	  * @param model
	  * @param hdfsStoreLocation
	 * @param partitionId 
	  * @return
	  */
	    public static String createLoadFolderPath(MolapLoadModel model,String hdfsStoreLocation, int partitionId, int currentRestructNumber)
	    {
	        hdfsStoreLocation = hdfsStoreLocation + File.separator + model.getSchemaName()+'_'+partitionId + File.separator
	                + model.getCubeName()+'_'+partitionId;

	        int rsCounter = currentRestructNumber/*MolapUtil.checkAndReturnNextRestructFolderNumber(hdfsStoreLocation,"RS_")*/;
	        if(rsCounter == -1)
	        {
	            rsCounter = 0;
	        }

	        String hdfsLoadedTable = hdfsStoreLocation + File.separator
	                + MolapCommonConstants.RESTRUCTRE_FOLDER + rsCounter + File.separator + model.getTableName();
	        
	        hdfsLoadedTable = hdfsLoadedTable.replace("\\", "/");

//	        int loadCounter = MolapUtil.checkAndReturnNextRestructFolderNumber(hdfsLoadedTable,"Load_");
	//
//	        String hdfsStoreLoadFolder = hdfsLoadedTable + File.separator + MolapCommonConstants.LOAD_FOLDER
//	                + loadCounter;
	        
	        return hdfsLoadedTable;
	    }
	    
	    public static MolapFile[] getAggregateTableList(final MolapLoadModel model,String hdfsStoreLocation, int partitionId, int currentRestructNumber)
	    {
		hdfsStoreLocation = hdfsStoreLocation + File.separator
				+ model.getSchemaName() + '_' + partitionId + File.separator
				+ model.getCubeName() + '_' + partitionId;

		int rsCounter = currentRestructNumber/*MolapUtil.checkAndReturnNextRestructFolderNumber(
				hdfsStoreLocation, "RS_")*/;
		if (rsCounter == -1) {
			rsCounter = 0;
		}

		String hdfsLoadedTable = hdfsStoreLocation + File.separator
				+ MolapCommonConstants.RESTRUCTRE_FOLDER + rsCounter;
		
		MolapFile rsFile = FileFactory.getMolapFile(hdfsLoadedTable, FileFactory.getFileType(hdfsLoadedTable));
		
		MolapFile[] aggFiles = rsFile.listFiles(new MolapFileFilter() {
			
			@Override
			public boolean accept(MolapFile file) {
				return file.getName().startsWith(MolapCommonConstants.AGGREGATE_TABLE_START_TAG
						+ MolapCommonConstants.UNDERSCORE + model.getTableName());
			}
		});

		return aggFiles;
	    }
}
