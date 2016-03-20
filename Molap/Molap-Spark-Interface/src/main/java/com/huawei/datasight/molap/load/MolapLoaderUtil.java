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

/**
 * 
 */
package com.huawei.datasight.molap.load;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
//import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;



import java.util.Set;


//import com.huawei.unibi.platform.api.molap.IDataProcessStatus;
//import com.huawei.unibi.platform.api.molap.dataloader.DataLoadModel;
//import com.huawei.unibi.platform.api.molap.dataloader.SchemaInfo;
//import com.huawei.unibi.repository.molap.DataProcessTaskStatus;
//import mondrian.olap.MondrianDef.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.gson.Gson;
import com.huawei.datasight.molap.core.load.LoadMetadataDetails;
import com.huawei.datasight.molap.spark.util.MolapSparkInterFaceLogEvent;
import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.api.dataloader.DataLoadModel;
import com.huawei.unibi.molap.api.dataloader.SchemaInfo;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.csvload.DataGraphExecuter;
import com.huawei.unibi.molap.dataprocessor.DataProcessTaskStatus;
import com.huawei.unibi.molap.dataprocessor.IDataProcessStatus;
import com.huawei.unibi.molap.datastorage.store.fileperations.AtomicFileOperations;
import com.huawei.unibi.molap.datastorage.store.fileperations.AtomicFileOperationsImpl;
import com.huawei.unibi.molap.datastorage.store.fileperations.FileWriteOperation;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFileFilter;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.engine.datastorage.InMemoryCube;
import com.huawei.unibi.molap.engine.datastorage.InMemoryCubeStore;
import com.huawei.unibi.molap.engine.directinterface.impl.MolapQueryParseUtil;
import com.huawei.unibi.molap.globalsurrogategenerator.GlobalSurrogateGenerator;
import com.huawei.unibi.molap.globalsurrogategenerator.GlobalSurrogateGeneratorInfo;
import com.huawei.unibi.molap.graphgenerator.GraphGenerator;
import com.huawei.unibi.molap.graphgenerator.GraphGeneratorException;
import com.huawei.unibi.molap.metadata.MolapMetadata;
import com.huawei.unibi.molap.metadata.MolapMetadata.Cube;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.olap.MolapDef;
import com.huawei.unibi.molap.olap.MolapDef.AggLevel;
import com.huawei.unibi.molap.olap.MolapDef.AggMeasure;
import com.huawei.unibi.molap.olap.MolapDef.AggName;
import com.huawei.unibi.molap.olap.MolapDef.AggTable;
import com.huawei.unibi.molap.olap.MolapDef.CubeDimension;
import com.huawei.unibi.molap.olap.MolapDef.Level;
import com.huawei.unibi.molap.olap.MolapDef.Measure;
import com.huawei.unibi.molap.olap.MolapDef.Schema;
import com.huawei.unibi.molap.util.MolapCoreLogEvent;
import com.huawei.unibi.molap.util.MolapProperties;
import com.huawei.unibi.molap.util.MolapSchemaParser;
import com.huawei.unibi.molap.util.MolapUtil;
import com.huawei.unibi.molap.util.MolapUtilException;

public final class MolapLoaderUtil 
{
	
	private static final LogService LOGGER = LogServiceFactory
            .getLogService(MolapLoaderUtil.class.getName());
	private MolapLoaderUtil()
	{
		
	}
	
    private static void generateGraph(IDataProcessStatus schmaModel, SchemaInfo info,
            String tableName,String partitionID, Schema schema, String factStoreLocation, int currentRestructNumber)
            throws GraphGeneratorException
    {
        DataLoadModel model = new DataLoadModel();
        model.setCsvLoad(null != schmaModel.getCsvFilePath() || null != schmaModel.getFilesToProcess());
        model.setSchemaInfo(info);
        model.setTableName(schmaModel.getTableName());
        boolean hdfsReadMode = schmaModel.getCsvFilePath()!=null && schmaModel.getCsvFilePath().startsWith("hdfs:");
       // System.out.println("RAMANA---hdfs load data path: " + schmaModel.getCsvFilePath());
        int allocate = null != schmaModel.getCsvFilePath() ? 1 : schmaModel.getFilesToProcess().size();
        GraphGenerator generator = new GraphGenerator(model, hdfsReadMode, partitionID, schema, factStoreLocation, currentRestructNumber,allocate);
        generator.generateGraph();
    }

    public static void executeGraph(MolapLoadModel loadModel, String storeLocation, String hdfsStoreLocation, String kettleHomePath, int currentRestructNumber) throws Exception
    {
        System.setProperty("KETTLE_HOME", kettleHomePath);
        if(!new File(storeLocation).mkdirs())
        {
        	LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "Error while new File(storeLocation).mkdirs() ");
        }
		 String outPutLoc = storeLocation+"/etl";
		 String schemaName = loadModel.getSchemaName();
	     String cubeName = loadModel.getCubeName();
	     String tempLocationKey = schemaName+'_'+cubeName;
	     MolapProperties.getInstance().addProperty(tempLocationKey, storeLocation);
		 MolapProperties.getInstance().addProperty(MolapCommonConstants.STORE_LOCATION_HDFS, hdfsStoreLocation);
		 MolapProperties.getInstance().addProperty("store_output_location", outPutLoc);
		 MolapProperties.getInstance().addProperty("send.signal.load", "false");
		 
		
		String tableName = loadModel.getTableName();
		String fileNamePrefix="";
		if(loadModel.isAggLoadRequest())
		{
			tableName = loadModel.getAggTableName();
			fileNamePrefix="graphgenerator";
		}
		String graphPath = outPutLoc + '/' + loadModel.getSchemaName()
		        + '/' + loadModel.getCubeName() + '/' + tableName.replaceAll(",", "") + fileNamePrefix + ".ktr";
		File path = new File(graphPath);
		if(path.exists())
		{
			path.delete();
		}
		
		DataProcessTaskStatus schmaModel = new DataProcessTaskStatus(schemaName, cubeName, tableName);
    	schmaModel.setCsvFilePath(loadModel.getFactFilePath());
    	schmaModel.setDimCSVDirLoc(loadModel.getDimFolderPath());
    	if(loadModel.isDirectLoad())
    	{
    		schmaModel.setFilesToProcess(loadModel.getFactFilesToProcess());
    		schmaModel.setDirectLoad(true);
    		schmaModel.setCsvDelimiter(loadModel.getCsvDelimiter());
    		schmaModel.setCsvHeader(loadModel.getCsvHeader());
    	}
    	SchemaInfo info = new SchemaInfo();
    	
    	info.setSchemaName(schemaName);
    	info.setSrcDriverName(loadModel.getDriverClass());
    	info.setSrcConUrl(loadModel.getJdbcUrl());
    	info.setSrcUserName(loadModel.getDbUserName());
    	info.setSrcPwd(loadModel.getDbPwd());
    	info.setCubeName(cubeName);
    	info.setSchemaPath(loadModel.getSchemaPath());
    	info.setAutoAggregateRequest(loadModel.isAggLoadRequest());
    	info.setComplexDelimiterLevel1(loadModel.getComplexDelimiterLevel1());
    	info.setComplexDelimiterLevel2(loadModel.getComplexDelimiterLevel2());
    	
    	generateGraph(schmaModel, info, loadModel.getTableName(),loadModel.getPartitionId(), loadModel.getSchema(), loadModel.getFactStoreLocation(), currentRestructNumber);
    	
       	DataGraphExecuter graphExecuter = new DataGraphExecuter(schmaModel);
    	graphExecuter.executeGraph(graphPath,new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN),info,loadModel.getPartitionId(),loadModel.getSchema());
    }

    public static String[] getStorelocs(String schemaName, String cubeName,
            String factTableName, String hdfsStoreLocation, int currentRestructNumber)
    {
        String[] loadFolders;

        String baseStorelocation = hdfsStoreLocation + File.separator
                + schemaName + File.separator + cubeName;

        String factStorepath = baseStorelocation + File.separator
                + MolapCommonConstants.RESTRUCTRE_FOLDER + currentRestructNumber
                + File.separator + factTableName;

        // Change to LOCAL for testing in local
        MolapFile file = FileFactory.getMolapFile(factStorepath,
                FileFactory.getFileType(factStorepath));

        if(!file.exists())
        {
            return new String[0];
        }
        MolapFile[] listFiles = file.listFiles(new MolapFileFilter()
        {
            @Override
            public boolean accept(MolapFile path)
            {
                return path.getName().startsWith(
                        MolapCommonConstants.LOAD_FOLDER)
                        && !path.getName().endsWith(
                                MolapCommonConstants.FILE_INPROGRESS_STATUS);
            }
        });

        loadFolders = new String[listFiles.length];
        int count = 0;

        for(MolapFile loadFile : listFiles)
        {
            loadFolders[count++] = loadFile.getAbsolutePath();
        }

        return loadFolders;
    }

    public static List<String> addNewSliceNameToList(String newSlice,
            List<String> activeSlices)
    {
        activeSlices.add(newSlice);
        return activeSlices;
    }

    public static String getAggLoadFolderLocation(String loadFolderName,
            String schemaName, String cubeName, String aggTableName,
            String hdfsStoreLocation, int currentRestructNumber)
    {
        for(int i = currentRestructNumber;i >= 0;i--)
        {
            String aggTableLocation = getTableLocation(schemaName, cubeName,
                    aggTableName, hdfsStoreLocation, i);
            String aggStorepath = aggTableLocation + File.separator
                    + loadFolderName;
            try
            {
                if(FileFactory.isFileExist(aggStorepath,
                        FileFactory.getFileType(aggStorepath)))
                {
                    return aggStorepath;
                }
            }
            catch(IOException e)
            {
                LOGGER.error(
                        MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,
                        "Problem checking file existence :: " + e.getMessage());
            }
        }
        return null;
    }

    public static String getTableLocation(String schemaName,
            String cubeName, String aggTableName, String hdfsStoreLocation,
            int currentRestructNumber)
    {
        String baseStorelocation = hdfsStoreLocation + File.separator
                + schemaName + File.separator + cubeName;
        int restructFolderNumber = currentRestructNumber/*MolapUtil
                .checkAndReturnNextRestructFolderNumber(baseStorelocation,
                        "RS_")*/;

        String aggTableLocation = baseStorelocation + File.separator
                        + MolapCommonConstants.RESTRUCTRE_FOLDER + restructFolderNumber
                        + File.separator + aggTableName;
        return aggTableLocation;
    }

    public static void deleteTable(int partitionCount,
            String schemaName, String cubeName, String aggTableName,
            String hdfsStoreLocation, int currentRestructNumber)
    {
        String aggTableLoc = null;
        String partitionSchemaName = null;
        String partitionCubeName = null;
        for(int i = 0;i < partitionCount;i++)
        {
            partitionSchemaName = schemaName + '_' + i;
            partitionCubeName = cubeName + '_' + i;
            for(int j = currentRestructNumber;j >= 0;j--)
            {
                aggTableLoc = getTableLocation(partitionSchemaName,
                        partitionCubeName, aggTableName, hdfsStoreLocation, j);
                deleteStorePath(aggTableLoc);
            }
        }
    }

    public static void deleteSlice(int partitionCount,
            String schemaName, String cubeName, String tableName,
            String hdfsStoreLocation, int currentRestructNumber, String loadFolder)
    {
        String tableLoc = null;
        String partitionSchemaName = null;
        String partitionCubeName = null;
        for(int i = 0;i < partitionCount;i++)
        {
            partitionSchemaName = schemaName + '_' + i;
            partitionCubeName = cubeName + '_' + i;
            tableLoc = getTableLocation(partitionSchemaName,
                    partitionCubeName, tableName, hdfsStoreLocation,
                    currentRestructNumber);
            tableLoc = tableLoc + File.separator + loadFolder;
            deleteStorePath(tableLoc);
        }
    }

    public static void deletePartialLoadDataIfExist(int partitionCount,
            String schemaName, String cubeName, String tableName,
            String hdfsStoreLocation, int currentRestructNumber, int loadFolder) throws IOException
    {
        String tableLoc = null;
        String partitionSchemaName = null;
        String partitionCubeName = null;
        for(int i = 0;i < partitionCount;i++)
        {
            partitionSchemaName = schemaName + '_' + i;
            partitionCubeName = cubeName + '_' + i;
            tableLoc = getTableLocation(partitionSchemaName,
                    partitionCubeName, tableName, hdfsStoreLocation,
                    currentRestructNumber);
            //tableLoc = tableLoc + File.separator + loadFolder;
            
            final List<String> loadFolders = new ArrayList<String>();
            for(int j=0;j<loadFolder;j++)
            {
            	loadFolders.add((tableLoc+File.separator+MolapCommonConstants.LOAD_FOLDER+j).replace("\\", "/"));
            }
            if(loadFolder != 0)
            {
            	loadFolders.add((tableLoc+File.separator+MolapCommonConstants.SLICE_METADATA_FILENAME+"."+currentRestructNumber).replace("\\", "/"));
            }
            FileType fileType = FileFactory.getFileType(tableLoc);
            if(FileFactory.isFileExist(tableLoc, fileType))
            {
            	MolapFile molapFile = FileFactory.getMolapFile(tableLoc, fileType);
                MolapFile[] listFiles = molapFile.listFiles(new MolapFileFilter()
                {
                    @Override
                    public boolean accept(MolapFile path)
                    {
                        return !loadFolders.contains(path.getAbsolutePath().replace("\\", "/")) && !path.getName().contains(MolapCommonConstants.MERGERD_EXTENSION);
                    }
                });
                for(int k = 0;k < listFiles.length;k++)
                {
                	deleteStorePath(listFiles[k].getAbsolutePath());
                }
            }
            
        }
    }

    public static void deleteStorePath(String path)
    {
        try
        {
            FileType fileType = FileFactory.getFileType(path);
            if(FileFactory.isFileExist(path, fileType))
            {
                MolapFile molapFile = FileFactory.getMolapFile(path, fileType);
                MolapUtil.deleteFoldersAndFiles(molapFile);
            }
        }
        catch(IOException e)
        {
            LOGGER.error(
                    MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,
                    "Unable to delete the given path :: " + e.getMessage());
        }
        catch(MolapUtilException e)
        {
            LOGGER.error(
                    MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,
                    "Unable to delete the given path :: " + e.getMessage());
        }
    }

    public static boolean isSliceValid(String loc, List<String> activeSlices,
            List<String> updatedSlices, String factTableName)
    {
        String loadFolderName = loc.substring(loc
                .indexOf(MolapCommonConstants.LOAD_FOLDER));
        String sliceNum = loadFolderName
                .substring(MolapCommonConstants.LOAD_FOLDER.length());
        if(activeSlices.contains(loadFolderName)
                || updatedSlices.contains(sliceNum))
        {
            String factFileLoc = loc + File.separator + factTableName + "_0"
                    + MolapCommonConstants.FACT_FILE_EXT;
            try
            {
                if(FileFactory.isFileExist(factFileLoc,
                        FileFactory.getFileType(factFileLoc)))
                {
                    return true;
                }
            }
            catch(IOException e)
            {
                LOGGER.error(
                        MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,
                        "Problem checking file existence :: " + e.getMessage());
            }
        }
        return false;
    }

    public static List<String> getListOfValidSlices(
            LoadMetadataDetails[] details)
    {
        List<String> activeSlices = new ArrayList<>(
                MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        for(LoadMetadataDetails oneLoad : details)
        {
            if(MolapCommonConstants.STORE_LOADSTATUS_SUCCESS.equals(oneLoad
                    .getLoadStatus())
                    || MolapCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS
                            .equals(oneLoad.getLoadStatus())
                    || MolapCommonConstants.MARKED_FOR_UPDATE.equals(oneLoad
                            .getLoadStatus()))
            {
                if(null != oneLoad.getMergedLoadName())
                {
                String loadName = MolapCommonConstants.LOAD_FOLDER
                            + oneLoad.getMergedLoadName();
                    activeSlices.add(loadName);
                }
                else
                {
                    String loadName = MolapCommonConstants.LOAD_FOLDER
                        + oneLoad.getLoadName();
                activeSlices.add(loadName);
            }
        }
        }
        return activeSlices;
    }

    public static List<String> getListOfUpdatedSlices(
            LoadMetadataDetails[] details)
    {
        List<String> updatedSlices = new ArrayList<>(
                MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        for(LoadMetadataDetails oneLoad : details)
        {
            if(MolapCommonConstants.MARKED_FOR_UPDATE.equals(oneLoad
                    .getLoadStatus()))
            {
                if(null != oneLoad.getMergedLoadName())
                {
                    updatedSlices.add(oneLoad.getMergedLoadName());
                }
                else
                {
                updatedSlices.add(oneLoad.getLoadName());
            }
        }
        }
        return updatedSlices;
    }

    public static String getMetaDataFilePath(String schemaName,
            String cubeName, String hdfsStoreLocation)
    {
        String basePath = hdfsStoreLocation;
        String schemaPath = basePath.substring(0, basePath.lastIndexOf("/"));
        String metadataFilePath = schemaPath + "/schemas/" + schemaName + '/'
                + cubeName;
        return metadataFilePath;
    }

    public static void removeSliceFromMemory(String schemaName,
            String cubeName, String loadName)
    {
        List<InMemoryCube> activeSlices = InMemoryCubeStore.getInstance()
                .getActiveSlices(schemaName + '_' + cubeName);
        Iterator<InMemoryCube> sliceItr = activeSlices.iterator();
        InMemoryCube slice = null;
        while(sliceItr.hasNext())
        {
            slice = sliceItr.next();
            if(loadName.equals(slice.getLoadName()))
            {
                sliceItr.remove();
            }
        }
    }

    public static boolean aggTableAlreadyExistWithSameMeasuresndLevels(
            AggName aggName, AggTable[] aggTables)
    {
        AggMeasure[] aggMeasures = null;
        AggLevel[] aggLevels = null;
        for(int i = 0;i < aggTables.length;i++)
        {
            aggLevels = aggTables[i].levels;
            aggMeasures = aggTables[i].measures;
            if(aggLevels.length == aggName.levels.length
                    && aggMeasures.length == aggName.measures.length)
            {
                if(checkforLevels(aggLevels, aggName.levels)
                        && checkforMeasures(aggMeasures, aggName.measures))
                {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean checkforLevels(AggLevel[] aggTableLevels,
            AggLevel[] newTableLevels)
    {
        int count = 0;
        for(int i = 0;i < aggTableLevels.length;i++)
        {
            for(int j = 0;j < newTableLevels.length;j++)
            {
                if(aggTableLevels[i].name.equals(newTableLevels[j].name))
                {
                    count++;
                    break;
                }
            }
        }
        if(count == aggTableLevels.length)
        {
            return true;
        }
        return false;
    }

    private static boolean checkforMeasures(AggMeasure[] aggMeasures,
            AggMeasure[] newTableMeasures)
    {
        int count = 0;
        for(int i = 0;i < aggMeasures.length;i++)
        {
            for(int j = 0;j < newTableMeasures.length;j++)
            {
                if(aggMeasures[i].name.equals(newTableMeasures[j].name)
                        && aggMeasures[i].aggregator
                                .equals(newTableMeasures[j].aggregator))
                {
                    count++;
                    break;
                }
            }
        }
        if(count == aggMeasures.length)
        {
            return true;
        }
        return false;
    }

    public static String getAggregateTableName(AggTable table)
    {
        return ((MolapDef.AggName)table).getNameAttribute();
    }

    public static void createEmptyLoadFolder(MolapLoadModel model,
            String factLoadFolderLocation, String hdfsStoreLocation, int currentRestructNumber)
    {
        String loadFolderName = factLoadFolderLocation
                .substring(factLoadFolderLocation
                        .indexOf(MolapCommonConstants.LOAD_FOLDER));
        String aggLoadFolderLocation = getTableLocation(
                model.getSchemaName(), model.getCubeName(),
                model.getAggTableName(), hdfsStoreLocation,
                currentRestructNumber);
        aggLoadFolderLocation = aggLoadFolderLocation + File.separator
                + loadFolderName;
        FileType fileType = FileFactory.getFileType(hdfsStoreLocation);
        try
        {
            FileFactory.mkdirs(aggLoadFolderLocation, fileType);
        }
        catch(IOException e)
        {
            LOGGER.error(
                    MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,
                    "Problem creating empty folder created for aggregation table :: "
                            + e.getMessage());
        }
        LOGGER.info(
                MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,
                "Empty folder created for aggregation table");
    }
    
    public static void copyCurrentLoadToHDFS(MolapLoadModel loadModel, int currentRestructNumber, String loadName, List<String> updatedSlices, int sliceRestructNumber) throws IOException, MolapUtilException
    {
      //Copy the current load folder to HDFS
        boolean copyStore = Boolean.valueOf(MolapProperties.getInstance().getProperty("dataload.hdfs.copy", "true"));
        
        String schemaName =  loadModel.getSchemaName();
        String cubeName =  loadModel.getCubeName();
        String factTable = loadModel.getTableName();
        String aggTableName = loadModel.getAggTableName();
        
        if(copyStore)
        {
            String hdfsLocation = MolapProperties.getInstance().getProperty(MolapCommonConstants.STORE_LOCATION_HDFS);
            String tempLocationKey = schemaName+'_'+cubeName;
            String localStore = MolapProperties.getInstance().getProperty(
                    tempLocationKey, MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL);
            if(!loadModel.isAggLoadRequest())
            {
                copyToHDFS(loadName, schemaName, cubeName, factTable,
                        hdfsLocation, localStore, currentRestructNumber, false, sliceRestructNumber);
            }
            if(null != aggTableName)
            {
                String sliceNumber = loadName.substring(MolapCommonConstants.LOAD_FOLDER.length());
                boolean isUpdate = false;
                if(updatedSlices.contains(sliceNumber))
                {
                    isUpdate = true;
                }
                copyToHDFS(loadName, schemaName, cubeName, aggTableName, hdfsLocation,
                        localStore, currentRestructNumber, isUpdate, sliceRestructNumber);
            }
                MolapUtil.deleteFoldersAndFiles(new File[] {
            	          new File(localStore + File.separator + schemaName + File.separator + cubeName) });
        }
    }
    public static void generateGlobalSurrogates(MolapLoadModel loadModel, String storeLocation, int numberOfPartiiton, String[] partitionColumn, CubeDimension[] dims, int currentRestructNumber)
    {
    	GlobalSurrogateGeneratorInfo generatorInfo= new GlobalSurrogateGeneratorInfo();
    	generatorInfo.setCubeName(loadModel.getCubeName());
    	generatorInfo.setSchema(loadModel.getSchema());
    	generatorInfo.setStoreLocation(storeLocation);
    	generatorInfo.setTableName(loadModel.getTableName());
    	generatorInfo.setNumberOfPartition(numberOfPartiiton);
    	generatorInfo.setPartiontionColumnName(partitionColumn[0]);
    	generatorInfo.setCubeDimensions(dims);
    	GlobalSurrogateGenerator generator = new GlobalSurrogateGenerator(generatorInfo);
    	generator.generateGlobalSurrogates(currentRestructNumber);
    }

	public static void copyToHDFS(String loadName, String schemaName, String cubeName,
        String factTable, String hdfsLocation, String localStore, int currentRestructNumber, boolean isUpdate, int sliceRestructNumber) throws IOException {
        //If the hdfs store and the local store configured differently, then copy
        if(hdfsLocation!=null && !hdfsLocation.equals(localStore))
        {
            /**
             * Identify the Load_X folder from thhe local store folder
             */
            String currentloadedStore = localStore;
            currentloadedStore = currentloadedStore + File.separator + schemaName + File.separator
                    + cubeName;

            int rsCounter = currentRestructNumber/*MolapUtil.checkAndReturnNextRestructFolderNumber(currentloadedStore,"RS_")*/;



            if(rsCounter == -1)
            {
                LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"Unable to find the local store details (RS_-1) " + currentloadedStore);
                return;
            }
            String localLoadedTable = currentloadedStore + File.separator
                    + MolapCommonConstants.RESTRUCTRE_FOLDER + rsCounter + File.separator + factTable;

            localLoadedTable = localLoadedTable.replace("\\", "/");

            int loadCounter = MolapUtil.checkAndReturnCurrentLoadFolderNumber(localLoadedTable);

            if(loadCounter == -1)
            {
                LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"Unable to find the local store details (Load_-1) " + currentloadedStore);

                return;
            }

            String localLoadFolder = localLoadedTable + File.separator + MolapCommonConstants.LOAD_FOLDER
                    + loadCounter;

            LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"Local data loaded folder ... = " + localLoadFolder);

            /**
             * Identify the Load_X folder in the HDFS store
             */

            if(isUpdate)
            {
                renameFactFile(localLoadFolder, factTable);
            }

            String hdfsStoreLocation= hdfsLocation;
            hdfsStoreLocation = hdfsStoreLocation + File.separator + schemaName + File.separator
                    + cubeName;

            rsCounter = currentRestructNumber;
            if(rsCounter == -1)
            {
                rsCounter = 0;
            }

            String hdfsLoadedTable = hdfsStoreLocation + File.separator
                    + MolapCommonConstants.RESTRUCTRE_FOLDER + rsCounter + File.separator + factTable;

            hdfsLoadedTable = hdfsLoadedTable.replace("\\", "/");

            String hdfsStoreLoadFolder = hdfsLoadedTable + File.separator + loadName;

            LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"HDFS data load folder ... = " + hdfsStoreLoadFolder);

            /**
             * Copy the data created through latest ETL run, to the HDFS store
             */

            LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"Copying " + localLoadFolder + " --> " + hdfsStoreLoadFolder);

            long time1 = System.currentTimeMillis();

            hdfsStoreLoadFolder = hdfsStoreLoadFolder.replace("\\", "/");
            Path path = new Path(hdfsStoreLocation);


            FileSystem fs = path.getFileSystem(FileFactory.getConfiguration());
            FileType fileType = FileFactory
                    .getFileType(hdfsStoreLoadFolder);
            if(FileFactory.isFileExist(hdfsStoreLoadFolder, fileType))
            {
                MolapFile molapFile = FileFactory.getMolapFile(
                        localLoadFolder,
                        FileFactory.getFileType(localLoadFolder));
                MolapFile[] listFiles = molapFile.listFiles();
                for(int i = 0;i < listFiles.length;i++)
                {
                    fs.copyFromLocalFile(true, true,
                            new Path(listFiles[i].getCanonicalPath()),
                            new Path(hdfsStoreLoadFolder));
                }
            }
            else
            {
                fs.copyFromLocalFile(true, true, new Path(localLoadFolder),
                        new Path(hdfsStoreLoadFolder));
            }

            LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"Copying sliceMetaData from " + localLoadedTable + " --> " + hdfsLoadedTable);


            /**
             *Handler:  Delete the Load_X folder from local path also can be done through the boolean parameter in  fs.copyFromLocalFile(...) call.
             */
            fs.copyFromLocalFile(true, true, new Path(localLoadedTable + ("/sliceMetaData" + "." + currentRestructNumber)), new Path(hdfsLoadedTable+ ("/sliceMetaData" + "." + sliceRestructNumber)));
            LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"Total HDFS copy time (ms):  " + (System.currentTimeMillis()-time1));

        }
        else
        {
            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "Separate molap.storelocation.hdfs is not configured for hdfs store path");
        }
	}

    private static void renameFactFile(String location, String tableName)
    {
        FileType fileType = FileFactory.getFileType(location);
        MolapFile molapFile = null;
        try
        {
            if(FileFactory.isFileExist(location, fileType))
            {
                molapFile = FileFactory.getMolapFile(location, fileType);
                MolapFile[] listFiles = molapFile
                        .listFiles(new MolapFileFilter()
                        {
                            @Override
                            public boolean accept(MolapFile path)
                            {
                                return path
                                        .getName()
                                        .endsWith(
                                                MolapCommonConstants.FACT_FILE_EXT)
                                        && !path.getName()
                                                .endsWith(
                                                        MolapCommonConstants.FILE_INPROGRESS_STATUS);
                            }
                        });
                for (int i = 0;i < listFiles.length;i++)
                {
                    molapFile = listFiles[i];
                    String factFilePath = molapFile.getCanonicalPath();
                    String changedFileName = factFilePath.replace(
                            MolapCommonConstants.FACT_FILE_EXT,
                            MolapCommonConstants.FACT_UPDATE_EXTENSION);
                    molapFile.renameTo(changedFileName);
                }
            }
        }
        catch(IOException e)
        {
            LOGGER.error(
                    MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,
                    "Inside renameFactFile. Problem checking file existence :: " + e.getMessage());
        }
    }

	
	/**
	 * API will provide the load number inorder to record the same in metadata file.
	 */
	public static int getLoadCount(MolapLoadModel loadModel, int currentRestructNumber)
        throws IOException {

            String hdfsLoadedTable=getLoadFolderPath(loadModel,null,null, currentRestructNumber);
            int loadCounter = MolapUtil
                    .checkAndReturnCurrentLoadFolderNumber(hdfsLoadedTable);

            String hdfsStoreLoadFolder = hdfsLoadedTable + File.separator
                    + MolapCommonConstants.LOAD_FOLDER + loadCounter;
            hdfsStoreLoadFolder = hdfsStoreLoadFolder.replace("\\", "/");

            String loadFolerCount = hdfsStoreLoadFolder.substring(
                    hdfsStoreLoadFolder.lastIndexOf('_')+1,
                    hdfsStoreLoadFolder.length());
               return Integer.parseInt(loadFolerCount)+1;
	}

	/**
	 * API will provide the load folder path for the store inorder to store the same
	 * in the metadata.
	 */
	public static String getLoadFolderPath(MolapLoadModel loadModel,String cubeName,String schemaName, int currentRestructNumber)
	{
		
        //CHECKSTYLE:OFF    Approval No:Approval-V1R2C10_005
		
		boolean copyStore = Boolean.valueOf(MolapProperties.getInstance()
				.getProperty("dataload.hdfs.copy", "true"));
		
		// CHECKSTYLE:ON
		if(null==cubeName && null==schemaName)
		{
		 schemaName = loadModel.getSchemaName();
		 cubeName = loadModel.getCubeName();
		}
		String factTable = loadModel.getTableName();
		String hdfsLoadedTable=null;
		if (copyStore) {
			String hdfsLocation = MolapProperties.getInstance().getProperty(
					MolapCommonConstants.STORE_LOCATION_HDFS);
			String localStore = MolapProperties.getInstance().getProperty(
					MolapCommonConstants.STORE_LOCATION,
					MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL);

			

			if (!hdfsLocation.equals(localStore)) {
				String hdfsStoreLocation = hdfsLocation;
				hdfsStoreLocation = hdfsStoreLocation + File.separator
						+ schemaName + File.separator + cubeName;

				int rsCounter = currentRestructNumber;
				if (rsCounter == -1) {
					rsCounter = 0;
				}

				 hdfsLoadedTable = hdfsStoreLocation + File.separator
						+ MolapCommonConstants.RESTRUCTRE_FOLDER + rsCounter
						+ File.separator + factTable;

				hdfsLoadedTable = hdfsLoadedTable.replace("\\", "/");
			}
		
		}
		return hdfsLoadedTable;
		
	}
	
	/**
	 * This API will write the load level metadata for the loadmanagement module inorder to
	 * manage the load and query execution management smoothly.
	 */
	public static void recordLoadMetadata(
			int loadCount, 
			LoadMetadataDetails loadMetadataDetails,
			MolapLoadModel loadModel,
			String loadStatus,
			String startLoadTime) throws IOException {

		String dataLoadLocation=null;
		//String dataLoadLocation = getLoadFolderPath(loadModel);
		
		dataLoadLocation =  extractLoadMetadataFileLocation(loadModel.getSchema(),
				loadModel.getSchemaName(), loadModel.getCubeName())+ File.separator + MolapCommonConstants.LOADMETADATA_FILENAME
                + MolapCommonConstants.MOLAP_METADATA_EXTENSION;
		Gson gsonObjectToRead = new Gson();
		List<LoadMetadataDetails> listOfLoadFolderDetails =null;
		DataInputStream dataInputStream =null;
		String loadEnddate = readCurrentTime();
		loadMetadataDetails.setTimestamp(loadEnddate);
		loadMetadataDetails.setLoadStatus(loadStatus);
		loadMetadataDetails.setLoadName(String.valueOf(loadCount));
		loadMetadataDetails.setLoadStartTime(startLoadTime);
		LoadMetadataDetails[] listOfLoadFolderDetailsArray =null;
		try {
			if(FileFactory.isFileExist(dataLoadLocation, FileFactory.getFileType(dataLoadLocation)))
			{

				 dataInputStream = FileFactory.getDataInputStream(
						dataLoadLocation, FileFactory.getFileType(dataLoadLocation));

				BufferedReader buffReader = new BufferedReader(
						new InputStreamReader(dataInputStream, MolapCommonConstants.MOLAP_DEFAULT_STREAM_ENCODEFORMAT));
				listOfLoadFolderDetailsArray = gsonObjectToRead.fromJson(buffReader,
						LoadMetadataDetails[].class);
            }
            listOfLoadFolderDetails=new ArrayList<LoadMetadataDetails>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
			
			if(null!=listOfLoadFolderDetailsArray)
			{
			for(LoadMetadataDetails loadMetadata:listOfLoadFolderDetailsArray)
			{
				listOfLoadFolderDetails.add(loadMetadata);
			}
			}
			listOfLoadFolderDetails.add(loadMetadataDetails);
			
		} 

		finally
		{

		   MolapUtil.closeStreams(dataInputStream);
		}
		writeLoadMetadata(
		        loadModel.getSchema(),
		        loadModel.getSchemaName(),
		        loadModel.getCubeName(),
		        listOfLoadFolderDetails);

	}

	public static void writeLoadMetadata(
			Schema schema,
			String schemaName,
			String cubeName,
			List<LoadMetadataDetails> listOfLoadFolderDetails)
			throws IOException {
	
		
		String dataLoadLocation =  extractLoadMetadataFileLocation(schema,
				schemaName, cubeName)+ File.separator + MolapCommonConstants.LOADMETADATA_FILENAME
                + MolapCommonConstants.MOLAP_METADATA_EXTENSION;
		DataOutputStream dataOutputStream;
		Gson gsonObjectToWrite = new Gson();
		BufferedWriter brWriter = null;
		
		AtomicFileOperations writeOperation = new AtomicFileOperationsImpl(dataLoadLocation, FileFactory.getFileType(dataLoadLocation) );
		
		try {
		    
		    dataOutputStream = writeOperation.openForWrite(FileWriteOperation.OVERWRITE);
			brWriter = new BufferedWriter(new OutputStreamWriter(
					dataOutputStream,
					MolapCommonConstants.MOLAP_DEFAULT_STREAM_ENCODEFORMAT));

			String metadataInstance = gsonObjectToWrite
					.toJson(listOfLoadFolderDetails.toArray());
			brWriter.write(metadataInstance);
		} finally {
			try {
				if (null != brWriter) {
					brWriter.flush();
				}
			} catch (Exception e) {
				LOGGER.error(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"error in  flushing ", e, e.getMessage());
				  			
			}
			MolapUtil.closeStreams(brWriter);
			
		}
		writeOperation.close();

	}

    public static String extractLoadMetadataFileLocation(
    		Schema schema, 
    		String schemaName , 
    		String cubeName)
    {
        Cube cube = MolapMetadata.getInstance().getCube(schemaName+'_'+cubeName);
		if(null == cube)
		{
			//Schema schema = loadModel.getSchema();
			com.huawei.unibi.molap.olap.MolapDef.Cube mondrianCube = MolapSchemaParser.getMondrianCube(schema, cubeName);
			MolapMetadata.getInstance().loadCube(schema,schema.name,mondrianCube.name, mondrianCube);
			cube = MolapMetadata.getInstance().getCube(schemaName+'_'+cubeName);
		}
		
		
		
        return cube.getMetaDataFilepath();
    }

	public static String readCurrentTime() {
		SimpleDateFormat sdf = new SimpleDateFormat(MolapCommonConstants.MOLAP_TIMESTAMP);
		String date = null;

		date = sdf.format(new Date());

		return date;
		}

	public static CubeDimension[][] getDimensionSplit(Schema schema, String cubeName, int numberOfPartition)
	{
		CubeDimension[] allDims=MolapSchemaParser.getMondrianCube(schema, cubeName).dimensions;
		Measure[] msrs=MolapSchemaParser.getMondrianCube(schema, cubeName).measures;
		List<CubeDimension> selectedDims= new ArrayList<CubeDimension>(MolapCommonConstants.CONSTANT_SIZE_TEN);
		for(int i = 0;i < allDims.length;i++)
        {
		    Level extractHierarchies = MolapSchemaParser.extractHierarchies(schema, allDims[i])[0].levels[0];
            for(int j = 0;j < msrs.length;j++)
            {
                if(extractHierarchies.column.equals(msrs[j].column))
                {
                    selectedDims.add(allDims[i]);
                }
            }
        }
		allDims=selectedDims.toArray(new CubeDimension[selectedDims.size()]);
		if(allDims.length<1)
		{
			return new CubeDimension[0][0];
		}
		int[] numberOfNodeToScanForEachThread = getNumberOfNodeToScanForEachThread(allDims.length,numberOfPartition);
		CubeDimension [][] out = new CubeDimension[numberOfNodeToScanForEachThread.length][];
		int counter=0;
	
		for (int i = 0; i < numberOfNodeToScanForEachThread.length; i++) 
		{
			out[i]=new CubeDimension[numberOfNodeToScanForEachThread[i]];
			for (int j = 0; j < numberOfNodeToScanForEachThread[i]; j++)
			{
				out[i][j]=allDims[counter++];
			}
		}
		
		return out;
	}

	private static int[] getNumberOfNodeToScanForEachThread(int numberOfNodes, int numberOfCores)
    {
		int div = numberOfNodes / numberOfCores;
		int mod = numberOfNodes % numberOfCores;
		int[] numberOfNodeToScan = null;
        if(div > 0)
        {
            numberOfNodeToScan = new int[numberOfCores];
            Arrays.fill(numberOfNodeToScan, div);
        }
        else if(mod > 0)
        {
            numberOfNodeToScan = new int[(int)mod];
        }
        for(int i = 0;i < mod;i++)
        {
            numberOfNodeToScan[i] = numberOfNodeToScan[i] + 1;
        }
        return numberOfNodeToScan;
    }

    public static String extractLoadMetadataFileLocation(
            MolapLoadModel loadModel)
    {
        Cube cube = MolapMetadata.getInstance().getCube(
                loadModel.getSchemaName() + '_' + loadModel.getCubeName());
        if(null == cube)
        {
            Schema schema = loadModel.getSchema();
            com.huawei.unibi.molap.olap.MolapDef.Cube mondrianCube = MolapSchemaParser
                    .getMondrianCube(schema, loadModel.getCubeName());
            MolapMetadata.getInstance().loadCube(schema,schema.name,mondrianCube.name, mondrianCube);
            cube = MolapMetadata.getInstance().getCube(
                    loadModel.getSchemaName() + '_' + loadModel.getCubeName());
        }

        return cube.getMetaDataFilepath();
    }
    
    public static int getCurrentRestructFolder(String schemaName, String cubeName, Schema schema)
    {
        Cube cube = MolapMetadata.getInstance().getCube(
                schemaName + '_' + cubeName);
        if(null == cube)
        {
            com.huawei.unibi.molap.olap.MolapDef.Cube mondrianCube = MolapSchemaParser
                    .getMondrianCube(schema, cubeName);
            MolapMetadata.getInstance().loadCube(schema,schema.name,mondrianCube.name, mondrianCube);
            cube = MolapMetadata.getInstance().getCube(
                    schemaName + '_' + cubeName);
        }

        String metaDataPath = cube.getMetaDataFilepath();
        int currentRestructNumber = MolapUtil
                .checkAndReturnCurrentRestructFolderNumber(metaDataPath, "RS_",
                        false);
        if(-1 == currentRestructNumber)
        {
            currentRestructNumber = 0;
        }

        return currentRestructNumber;
    }
    
    /**
     * This method will provide the dimension column list for a given aggregate
     * table
     */
    public static Set<String> getColumnListFromAggTable(MolapLoadModel model)
    {
        Set<String> columnList = new HashSet<String>(
                MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        Cube metadataCube = MolapMetadata.getInstance().getCube(
                model.getSchemaName() + '_' + model.getCubeName());
        MolapDef.Cube cube = model.getSchema().cubes[0];
        MolapDef.Table factTable = (MolapDef.Table)cube.fact;
        List<Dimension> dimensions = metadataCube.getDimensions(factTable.name);
        MolapDef.AggTable[] aggTables = factTable.getAggTables();
        String aggTableName = null;
        AggLevel[] aggLevels = null;
        AggMeasure[] aggMeasures = null;
        for(int i = 0;i < aggTables.length;i++)
        {
            aggTableName = ((MolapDef.AggName)aggTables[i]).getNameAttribute();
            if(model.getAggTableName().equals(aggTableName))
            {
                aggLevels = aggTables[i].levels;
                aggMeasures = aggTables[i].measures;
                for(AggLevel aggDim : aggLevels)
                {
                    Dimension dim = MolapQueryParseUtil.findDimension(
                            dimensions, aggDim.column);
                    if(null != dim)
                    {
                        columnList.add(dim.getColName());
                    }
                }
                for(AggMeasure aggMsr : aggMeasures)
                {
                    Dimension dim = MolapQueryParseUtil.findDimension(
                            dimensions, aggMsr.column);
                    if(null != dim)
                    {
                        columnList.add(dim.getColName());
                    }
                }
                break;
            }
        }
        return columnList;
    }
	 public static void copyMergedLoadToHDFS(MolapLoadModel loadModel, int currentRestructNumber, String mergedLoadName)
    {
        //Copy the current load folder to HDFS
        boolean copyStore = Boolean.valueOf(MolapProperties.getInstance().getProperty("dataload.hdfs.copy", "true"));
        
        String schemaName =  loadModel.getSchemaName();
        String cubeName =  loadModel.getCubeName();
        String factTable = loadModel.getTableName();
        String aggTableName = loadModel.getAggTableName();
        
        if(copyStore)
        {
            String hdfsLocation = MolapProperties.getInstance().getProperty(MolapCommonConstants.STORE_LOCATION_HDFS);
            
            String localStore = MolapProperties.getInstance().getProperty(
                    MolapCommonConstants.STORE_LOCATION, MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL);
            if(!loadModel.isAggLoadRequest())
            {
            copyMergeToHDFS(schemaName, cubeName, factTable, hdfsLocation,localStore, currentRestructNumber, mergedLoadName);
            }
            if(null != aggTableName) {
                copyMergeToHDFS(schemaName, cubeName, aggTableName, hdfsLocation, localStore, currentRestructNumber,mergedLoadName);
            }
            try
            {
                 MolapUtil.deleteFoldersAndFiles(new File[] { 
                          new File(localStore + File.separator + schemaName + File.separator + cubeName) });
            } 
            catch (MolapUtilException e) 
            {
                LOGGER.error(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, "Error while MolapUtil.deleteFoldersAndFiles ", e, e.getMessage());
            }
        }
    }
    
    public static void copyMergeToHDFS(String schemaName, String cubeName,
            String factTable, String hdfsLocation, String localStore, int currentRestructNumber, String mergedLoadName) {
        try
        {
            //If the hdfs store and the local store configured differently, then copy
            if(hdfsLocation!=null && !hdfsLocation.equals(localStore))
            {
                /**
                 * Identify the Load_X folder from the local store folder
                 */
                String currentloadedStore = localStore;
                currentloadedStore = currentloadedStore + File.separator + schemaName + File.separator
                        + cubeName;


                int rsCounter = currentRestructNumber;

           

                if(rsCounter == -1)
                {
                    LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"Unable to find the local store details (RS_-1) " + currentloadedStore);
                    return;
                }
                String localLoadedTable = currentloadedStore + File.separator
                        + MolapCommonConstants.RESTRUCTRE_FOLDER + rsCounter + File.separator + factTable;

                localLoadedTable = localLoadedTable.replace("\\", "/");
                
                int loadCounter = MolapUtil.checkAndReturnCurrentLoadFolderNumber(localLoadedTable);

                if(loadCounter == -1)
                {
                    LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"Unable to find the local store details (Load_-1) " + currentloadedStore);
                       
                    return;
                }

                String localLoadName = MolapCommonConstants.LOAD_FOLDER
                        + mergedLoadName;
                String localLoadFolder = localLoadedTable + File.separator + MolapCommonConstants.LOAD_FOLDER
                        + mergedLoadName;
                
                LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"Local data loaded folder ... = " + localLoadFolder);
               
                /**
                 * Identify the Load_X folder in the HDFS store 
                 */
                
                String hdfsStoreLocation= hdfsLocation;
                hdfsStoreLocation = hdfsStoreLocation + File.separator + schemaName + File.separator
                        + cubeName;

                rsCounter = currentRestructNumber;
                if(rsCounter == -1)
                {
                    rsCounter = 0;
                }

                String hdfsLoadedTable = hdfsStoreLocation + File.separator
                        + MolapCommonConstants.RESTRUCTRE_FOLDER + rsCounter + File.separator + factTable;
                
                hdfsLoadedTable = hdfsLoadedTable.replace("\\", "/");

                String hdfsStoreLoadFolder = hdfsLoadedTable + File.separator + localLoadName;

                LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"HDFS data load folder ... = " + hdfsStoreLoadFolder);
                
                /**
                 * Copy the data created through latest ETL run, to the HDFS store 
                 */
                
                LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"Copying " + localLoadFolder + " --> " + hdfsStoreLoadFolder);
                
                hdfsStoreLoadFolder = hdfsStoreLoadFolder.replace("\\", "/");
                Path path = new Path(hdfsStoreLocation);
                
               
                FileSystem fs = path.getFileSystem(FileFactory.getConfiguration());
                fs.copyFromLocalFile(true, true, new Path(localLoadFolder), new Path(hdfsStoreLoadFolder));
                
                LOGGER.info(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG,"Copying sliceMetaData from " + localLoadedTable + " --> " + hdfsLoadedTable);
                
            }
            else
            {
                LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "Separate molap.storelocation.hdfs is not configured for hdfs store path");
            }
        }
        catch(Exception e)
        {
            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,e.getMessage());
        }
    }
}
