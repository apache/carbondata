package com.huawei.datasight.molap.merger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.huawei.datasight.molap.core.load.LoadMetadataDetails;
import com.huawei.datasight.molap.load.DeleteLoadFolders;
import com.huawei.datasight.molap.load.MolapLoadModel;
import com.huawei.datasight.molap.load.MolapLoaderUtil;
import com.huawei.datasight.molap.spark.util.LoadMetadataUtil;
import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFileFilter;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.merger.MolapColumnarSliceMerger;
import com.huawei.unibi.molap.merger.MolapSliceMergerInfo;
import com.huawei.unibi.molap.util.MolapCoreLogEvent;
import com.huawei.unibi.molap.util.MolapProperties;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * utility class for load merging.
 * @author R00903928
 *
 */
public final class MolapDataMergerUtil 
{
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(MolapDataMergerUtil.class.getName());
    
	private MolapDataMergerUtil()
	{
		
	}
	/**
	 * 
	 * @param molapLoadModel
	 * @param storeLocation
	 * @param hdfsStoreLocation
	 * @param currentRestructNumber
	 * @param metadataFilePath
	 * @param loadsToMerge
	 * @param mergedLoadName
	 * @return
	 * @throws Exception
	 */
	public static boolean executeMerging(MolapLoadModel molapLoadModel,String storeLocation, String hdfsStoreLocation, int currentRestructNumber , String metadataFilePath, List<String> loadsToMerge, String mergedLoadName) throws Exception
	{
		MolapProperties.getInstance().addProperty(
				MolapCommonConstants.STORE_LOCATION, storeLocation);
		MolapProperties.getInstance().addProperty(
				MolapCommonConstants.STORE_LOCATION_HDFS, hdfsStoreLocation);
		MolapSliceMergerInfo molapSliceMergerInfo = new MolapSliceMergerInfo();
		
		
		molapSliceMergerInfo.setCubeName(molapLoadModel.getCubeName());
		molapSliceMergerInfo.setPartitionID(molapLoadModel.getPartitionId());
		molapSliceMergerInfo.setSchema(molapLoadModel.getSchema());
		molapSliceMergerInfo.setSchemaName(molapLoadModel.getSchemaName());
		molapSliceMergerInfo.setSchemaPath(molapLoadModel.getSchemaPath());
		molapSliceMergerInfo.setTableName(molapLoadModel.getTableName());
		molapSliceMergerInfo.setMetadataPath(metadataFilePath);
		molapSliceMergerInfo.setLoadsToBeMerged(loadsToMerge);
		molapSliceMergerInfo.setMergedLoadName(mergedLoadName);
		
		MolapColumnarSliceMerger merger = new MolapColumnarSliceMerger(molapSliceMergerInfo);
		return merger.fullMerge(currentRestructNumber);
		
	}
	
	/**
	 * 
	 * @param storeLocation
	 * @param fileType
	 * @param metadataPath
	 * @param molapLoadModel
	 * @param currentRestructNumber
	 * @param partitionCount
	 * @return
	 */
	 public static List<String> getLoadsToMergeFromHDFS(String storeLocation, FileType fileType, String metadataPath, MolapLoadModel molapLoadModel,int currentRestructNumber,int partitionCount)
    {
        List<String> loadNames = new ArrayList<String>(
                MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

        try
        {
            if(!FileFactory.isFileExist(storeLocation, fileType))
            {
                return null;
            }
        }
        catch(IOException e)
        {
             LOGGER.error(
             MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG ,
             "Error occurred :: " + e.getMessage());
        }

        int toLoadMergeMaxSize;
        try
        {
            toLoadMergeMaxSize = Integer
                    .parseInt(MolapProperties
                            .getInstance()
                            .getProperty(
                                    MolapCommonConstants.TO_LOAD_MERGE_MAX_SIZE,
                                    MolapCommonConstants.TO_LOAD_MERGE_MAX_SIZE_DEFAULT));
        }
        catch(NumberFormatException e)
        {
            toLoadMergeMaxSize = Integer
                    .parseInt(MolapCommonConstants.TO_LOAD_MERGE_MAX_SIZE_DEFAULT);
        }

        LoadMetadataDetails[] loadDetails = MolapUtil
                .readLoadMetadata(metadataPath);

        for(LoadMetadataDetails loadDetail : loadDetails)
        {
            if(loadNames.size() < 2)
            {
                // check if load is not deleted.
                if(checkIfLoadIsNotDeleted(loadDetail))
                {
                    // check if load is merged
                    if(checkIfLoadIsMergedAlready(loadDetail))
                    {
                        if(checkSizeOfloadToMerge(loadDetail,
                                toLoadMergeMaxSize, molapLoadModel,
                                partitionCount, storeLocation,
                                currentRestructNumber,
                                loadDetail.getMergedLoadName()))
                        {
                            if(!loadNames.contains(loadDetail.getMergedLoadName()))
                            {
                                loadNames.add(loadDetail.getMergedLoadName());
                            }
                        }
                    }
                    else
                    // take this load as To Load.
                    {
                        if(checkSizeOfloadToMerge(loadDetail,
                                toLoadMergeMaxSize, molapLoadModel,
                                partitionCount, storeLocation,
                                currentRestructNumber, loadDetail.getLoadName()))
                        {
                            loadNames.add(loadDetail.getLoadName());
                        }
                    }
                }
            }
            else
            {
                break;
            }
        }
        
        return loadNames;

    }
	 
	/**
	 * 
	 * @param loadDetail
	 * @param toLoadMergeMaxSize
	 * @param molapLoadModel
	 * @param partitionCount
	 * @param storeLocation 
	 * @param currentRestructNumber 
	 * @return
	 */
	 private static boolean checkSizeOfloadToMerge(
            final LoadMetadataDetails loadDetail, int toLoadMergeMaxSize,MolapLoadModel molapLoadModel, int partitionCount, String storeLocation, int currentRestructNumber, final String loadNameToMatch)
    {

	     long factSizeAcrossPartition = 0;
	     
        for(int partition = 0;partition < partitionCount;partition++)
        {
            String loadPath = LoadMetadataUtil.createLoadFolderPath(
                    molapLoadModel, storeLocation, partition,
                    currentRestructNumber);

            MolapFile parentLoadFolder = FileFactory.getMolapFile(loadPath,
                    FileFactory.getFileType(loadPath));
            MolapFile[] loadFiles = parentLoadFolder
                    .listFiles(new MolapFileFilter()
                    {
                        @Override
                        public boolean accept(MolapFile file)
                        {
                            if(file.getName()
                                    .substring(file.getName().indexOf('_') + 1,
                                            file.getName().length())
                                    .equalsIgnoreCase(loadNameToMatch))
                            {
                                return true;
                            }
                            return false;
                        }
                    });
            
            // no found load folder in current RS
            if(loadFiles.length == 0)
            {
                return false;
            }
            
            // check if fact file is present or not. this is in case of Restructure folder.
            if(!isFactFilePresent(loadFiles[0]))
            {
                return false;
            }
            
            
             factSizeAcrossPartition += getSizeOfFactFileInLoad(loadFiles[0]);
            
            // MolapFile loadFolder = new MolapFile()
        }
        // check avg fact size if less than configured max size of to load.
        if(factSizeAcrossPartition < toLoadMergeMaxSize*1024*1024*1024)
        {
            return true;
        }

        // LoadMetadataUtil.createLoadFolderPath(model, hdfsStoreLocation,
        // partitionId, currentRestructNumber);
        return false;
    }
	 
    /**
	  * 
	  * @param molapFile
	  * @return
	  */
    private static long getSizeOfFactFileInLoad(MolapFile molapFile)
    {
        long factSize = 0;
        
        // check if update fact is present.
        
        MolapFile [] factFileUpdated = molapFile.listFiles(new MolapFileFilter()
        {
            
            @Override
            public boolean accept(MolapFile file)
            {
                if(file.getName().endsWith(MolapCommonConstants.FACT_UPDATE_EXTENSION))
                {
                    return true;
                }
                return false;
            }
        });
        
        if(factFileUpdated.length != 0)
        {
            for(MolapFile fact : factFileUpdated)
            {
                factSize += fact.getSize();
            }
            return factSize;
        }
        
        // normal fact case.
        MolapFile [] factFile = molapFile.listFiles(new MolapFileFilter()
        {
            
            @Override
            public boolean accept(MolapFile file)
            {
                if(file.getName().endsWith(MolapCommonConstants.FACT_FILE_EXT))
                {
                    return true;
                }
                return false;
            }
        });
        
        for(MolapFile fact  : factFile)
        {
            factSize +=  fact.getSize();
        }
        
        return factSize;
    }
    /**
	  * 
	  * @param loadDetail
	  * @return
	  */
	private static boolean checkIfLoadIsMergedAlready(
            LoadMetadataDetails loadDetail)
    {
	    if(null != loadDetail.getMergedLoadName())
	    {
	        return true;
	    }
        return false;
    }
    /**
	 *  
	 * @param loadDetail
	 */
    private static boolean checkIfLoadIsNotDeleted(LoadMetadataDetails loadDetail)
    {
        if(!loadDetail.getLoadStatus().equalsIgnoreCase(MolapCommonConstants.MARKED_FOR_DELETE))
        {
            return true;
        }
        else
        {
            return false;
        }
    }
    
    /**
     * 
     * @param metadataFilePath
     * @param molapLoadModel 
     * @param storeLocation 
     * @param partition 
     * @param currentRestructNumber 
     * @return
     */
    public static boolean checkIfLoadMergingRequired(String metadataFilePath, MolapLoadModel molapLoadModel, String storeLocation, int partition, int currentRestructNumber)
    {
        
        
        String loadPath = LoadMetadataUtil.createLoadFolderPath(
                molapLoadModel, storeLocation, 0,
                currentRestructNumber);

        MolapFile parentLoadFolder = FileFactory.getMolapFile(loadPath,
                FileFactory.getFileType(loadPath));
        
        // get all the load files in the current RS 
        MolapFile[] loadFiles = parentLoadFolder
                .listFiles(new MolapFileFilter()
                {
                    @Override
                    public boolean accept(MolapFile file)
                    {
                        if(file.getName()
                                .startsWith(MolapCommonConstants.LOAD_FOLDER ))
                        {
                            return true;
                        }
                        return false;
                    }
                });
        
        
        String isLoadMergeEnabled = MolapProperties.getInstance().getProperty(
                MolapCommonConstants.ENABLE_LOAD_MERGE,
                MolapCommonConstants.DEFAULT_ENABLE_LOAD_MERGE);
        
        if(isLoadMergeEnabled.equalsIgnoreCase("false"))
        {
            return false;
        }
        
        int mergeThreshold;
        try
        {
             mergeThreshold = Integer.parseInt(MolapProperties.getInstance().getProperty(
                MolapCommonConstants.MERGE_THRESHOLD_VALUE,
                MolapCommonConstants.MERGE_THRESHOLD_DEFAULT_VAL));
        }
        catch(NumberFormatException e)
        {
            mergeThreshold = Integer.parseInt(MolapCommonConstants.MERGE_THRESHOLD_DEFAULT_VAL);
        }
        
        LoadMetadataDetails [] details = MolapUtil.readLoadMetadata(metadataFilePath);
        
        int validLoadsNumber = getNumberOfValidLoads(details,loadFiles);
        
        if(validLoadsNumber > mergeThreshold+1)
        {
            return true;
        }
        else
        {
            return false;
        }
    }
    
    /**
     * 
     * @param details
     * @param loadFiles 
     * @return
     */
    private static int getNumberOfValidLoads(LoadMetadataDetails[] details, MolapFile[] loadFiles)
    {
        int validLoads = 0;
        
        for(LoadMetadataDetails load : details)
        {
            if(load.getLoadStatus().equalsIgnoreCase(MolapCommonConstants.STORE_LOADSTATUS_SUCCESS)
                    || load.getLoadStatus().equalsIgnoreCase(MolapCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS)
                    || load.getLoadStatus().equalsIgnoreCase(MolapCommonConstants.MARKED_FOR_UPDATE))
            {
                
                if(isLoadMetadataPresentInRsFolders(loadFiles,load.getLoadName()))
                {
                    validLoads++;
                }
            }
        }
        
        return validLoads;
    
    }
    
    /**
     * 
     * @param loadFiles
     * @param loadName
     * @return
     */
    private static boolean isLoadMetadataPresentInRsFolders(
            MolapFile[] loadFiles, String loadName)
    {
        for(MolapFile load : loadFiles)
        {
            String nameOfLoad = load.getName().substring(load.getName().indexOf(MolapCommonConstants.UNDERSCORE)+1, load.getName().length());
            if(nameOfLoad.equalsIgnoreCase(loadName))
            {
                // check if it is a RS load or not.
                MolapFile [] factFiles = load.listFiles(new MolapFileFilter()
                {
                    @Override
                    public boolean accept(MolapFile file)
                    {
                        if (file.getName().endsWith(MolapCommonConstants.FACT_FILE_EXT ) || file.getName().endsWith(MolapCommonConstants.FACT_UPDATE_EXTENSION ))
                        {
                            return true;
                        }
                        return false;
                    }
                });
                
                if(factFiles.length > 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            
        }
        return false;
    }
    /**
     * 
     * @param loadName
     * @return
     */
    public static String getMergedLoadName(List<String> loadName)
    {
         String mergeLoadName = loadName.get(0); 
        String timeStamp = new Date().getTime()+"";
        
        if(mergeLoadName.contains(MolapCommonConstants.MERGERD_EXTENSION))
        {
            String loadNum = mergeLoadName.substring(0, mergeLoadName.indexOf(MolapCommonConstants.MERGERD_EXTENSION));
            return loadNum+MolapCommonConstants.MERGERD_EXTENSION+MolapCommonConstants.UNDERSCORE+timeStamp;
        }
        else
        {
            return mergeLoadName+MolapCommonConstants.MERGERD_EXTENSION+MolapCommonConstants.UNDERSCORE+timeStamp;
        }
        
    }
    
    /**
     * 
     * @param loadsToMerge
     * @param metaDataFilepath
     * @param MergedLoadName
     */
    public static void updateLoadMetadataWithMergeStatus(List<String> loadsToMerge, String metaDataFilepath, String MergedLoadName,MolapLoadModel molapLoadModel)
    {
        LoadMetadataDetails[] loadDetails =  MolapUtil.readLoadMetadata(metaDataFilepath);
        
        boolean first = true;
        
        for(LoadMetadataDetails loadDetail : loadDetails)
        {
            
            if(null != loadDetail.getMergedLoadName())
            {
                if(loadsToMerge.contains(loadDetail.getMergedLoadName()) && first)
                {
                    loadDetail.setMergedLoadName(MergedLoadName);
                    first = false;
                }
                else
                {/*
                    loadDetail.setLoadStatus(MolapCommonConstants.MARKED_FOR_DELETE);
                    loadDetail.setDeletionTimestamp(MolapLoaderUtil
                            .readCurrentTime());
                */
                    continue;
                }
            }
            
            else if(loadsToMerge.contains(loadDetail.getLoadName()) )
            {
                if(first)
                {
                loadDetail.setMergedLoadName(MergedLoadName);
                first = false;
                }
                else
                {
                    loadDetail.setLoadStatus(MolapCommonConstants.MARKED_FOR_DELETE);
                    loadDetail.setDeletionTimestamp(MolapLoaderUtil
                            .readCurrentTime());
                }
                
            }
           
        }
        
        try
        {
            MolapLoaderUtil.writeLoadMetadata(molapLoadModel.getSchema(), molapLoadModel.getSchemaName(), molapLoadModel.getCubeName(), Arrays.asList(loadDetails));
        }
        catch(IOException e)
        {
            
        }
        
    }
    
    /**
     * 
     * @param path
     * @param loadModel
     */
    public static void cleanUnwantedMergeLoadFolder(
            MolapLoadModel loadModel, int partitionCount, String storeLocation,
            boolean isForceDelete, int currentRestructNumber)
    {

        String loadMetadataFilePath = MolapLoaderUtil
                .extractLoadMetadataFileLocation(loadModel);

        LoadMetadataDetails[] details = MolapUtil
                .readLoadMetadata(loadMetadataFilePath);
        
        // for first time before any load , this will be null
        if( null == details || details.length == 0  )
        {
            return;
        }

        for(int partitionId = 0;partitionId < partitionCount;partitionId++)
        {

            String path = LoadMetadataUtil.createLoadFolderPath(loadModel,
                    storeLocation, partitionId, currentRestructNumber);

            MolapFile loadFolder = FileFactory.getMolapFile(path,
                    FileFactory.getFileType(path));

            MolapFile[] loads = loadFolder.listFiles(new MolapFileFilter()
            {
                @Override
                public boolean accept(MolapFile file)
                {
                    if(file.getName().startsWith(
                            MolapCommonConstants.LOAD_FOLDER)
                            && file.getName().contains(
                                    MolapCommonConstants.MERGER_FOLDER_EXT))
                    {
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
            });

            for(int i = 0;i < loads.length;i++)
            {
                if(checkIfOldMergeLoadCanBeDeleted(loads[i], details))
                {
                    // delete merged load folder
                    MolapFile[] files = loads[i].listFiles();
                    // deleting individual files
                    if(files != null)
                    {
                        for(MolapFile eachFile : files)
                        {
                            if(!eachFile.delete())
                            {
                                LOGGER.warn(
                                        MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                                        "Unable to delete the file."
                                                + loadFolder.getAbsolutePath());
                            }
                        }

                        loads[i].delete();

                    }
                    
                    // delete corresponding aggregate table.
                    
                    MolapFile[] aggFiles = LoadMetadataUtil.getAggregateTableList(loadModel, storeLocation, partitionId, currentRestructNumber);
                    DeleteLoadFolders.deleteAggLoadFolders(aggFiles,loads[i].getName());
                    
                }
            }
        }
    }
    
    /**
     * 
     * @param eachMergeLoadFolder
     * @param details
     * @return
     */
    private static boolean checkIfOldMergeLoadCanBeDeleted(
            MolapFile eachMergeLoadFolder, LoadMetadataDetails[] details)
    {
        boolean found = false;
        for(LoadMetadataDetails loadDetail : details)
        {
            if(null != loadDetail.getMergedLoadName() && (MolapCommonConstants.LOAD_FOLDER+loadDetail.getMergedLoadName()).equalsIgnoreCase(eachMergeLoadFolder.getName()))
            {
                found = true;
                break;
            }
        }
        
        if(!found)
        {
            // check the query execution time out and check the time stamp on load and delete.
            
            String loadName = eachMergeLoadFolder.getName();
            long loadTime = Long.parseLong( loadName.substring( loadName.lastIndexOf(MolapCommonConstants.UNDERSCORE)+1,  loadName.length()));
            long currentTime = new Date().getTime();
            
            long millis = getMaxQueryTimeOut();
            
            if ((currentTime-loadTime) > millis)
            {
                // delete that merge load folder
                return true;
            }
        }
        
        return false;
    }
    /**
     * 
     * @return
     */
    private static long getMaxQueryTimeOut()
    {
        int maxTime; 
        try
        {
            maxTime = Integer.parseInt(MolapProperties.getInstance().
                    getProperty(MolapCommonConstants.MAX_QUERY_EXECUTION_TIME));
        }
        catch(NumberFormatException e)
        {
           maxTime = MolapCommonConstants.DEFAULT_MAX_QUERY_EXECUTION_TIME;
        }
        
        return maxTime*60000;
      
    }
    
    /**
     * 
     * @param molapFile
     * @return
     */
    private static boolean isFactFilePresent(MolapFile molapFile)
   {
        
        MolapFile [] factFileUpdated = molapFile.listFiles(new MolapFileFilter()
        {
            
            @Override
            public boolean accept(MolapFile file)
            {
                if(file.getName().endsWith(MolapCommonConstants.FACT_UPDATE_EXTENSION))
                {
                    return true;
                }
                return false;
            }
        });
        
        if(factFileUpdated.length != 0 )
        {
            return true;
        }
        
        MolapFile [] factFile = molapFile.listFiles(new MolapFileFilter()
        {
            
            @Override
            public boolean accept(MolapFile file)
            {
                if(file.getName().endsWith(MolapCommonConstants.FACT_FILE_EXT))
                {
                    return true;
                }
                return false;
            }
        });
        
        if(factFile.length != 0 )
        {
            return true;
        }
        
       return false;
   }
    
}
