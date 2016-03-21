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

package com.huawei.unibi.molap.restructure;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.ArrayUtils;
import org.pentaho.di.core.exception.KettleException;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFileFilter;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.keygenerator.factory.KeyGeneratorFactory;
import com.huawei.unibi.molap.metadata.SliceMetaData;
import com.huawei.unibi.molap.olap.MolapDef;
import com.huawei.unibi.molap.olap.MolapDef.CubeDimension;
import com.huawei.unibi.molap.olap.MolapDef.Dimension;
import com.huawei.unibi.molap.olap.MolapDef.Measure;
import com.huawei.unibi.molap.olap.MolapDef.RelationOrJoin;
import com.huawei.unibi.molap.olap.MolapDef.Table;
import com.huawei.unibi.molap.util.LevelSortIndexWriterThread;
import com.huawei.unibi.molap.util.MolapCoreLogEvent;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;
import com.huawei.unibi.molap.util.MolapProperties;
import com.huawei.unibi.molap.util.MolapSchemaParser;
import com.huawei.unibi.molap.util.MolapUtil;
import com.huawei.unibi.molap.util.MolapUtilException;


public class SchemaRestructurer
{
    /**
     * 
     * Comment for <code>LOGGER</code>
     * 
     */
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(SchemaRestructurer.class.getName());
    
    private static final int DEF_SURROGATE_KEY = 1;

    private String pathTillRSFolderParent;
    private String newRSFolderName;
    private String newSliceMetaDataPath;
    private String levelFilePrefix;
    private int nextRestructFolder = -1;
    private int currentRestructFolderNumber = -1;
    private String factTableName;
    private String cubeName;
    private String newSliceMetaDataFileExtn;
    private long curTimeToAppendToDroppedDims;
    
    public SchemaRestructurer(String schemaName, String cubeName, String factTableName, String hdfsStoreLocation, int currentRestructFolderNum, long curTimeToAppendToDroppedDims)
    {
        pathTillRSFolderParent = hdfsStoreLocation + File.separator + schemaName
                + File.separator + cubeName;
        this.currentRestructFolderNumber = currentRestructFolderNum;
        if(-1 == currentRestructFolderNumber)
        {
            currentRestructFolderNumber = 0;
        }
        nextRestructFolder = currentRestructFolderNumber + 1;
        
        newRSFolderName = MolapCommonConstants.RESTRUCTRE_FOLDER + nextRestructFolder;
        
        newSliceMetaDataPath = pathTillRSFolderParent + File.separator
                + newRSFolderName + File.separator + factTableName;
        
        this.factTableName = factTableName;
        this.cubeName = cubeName.substring(0, cubeName.lastIndexOf('_'));
        this.curTimeToAppendToDroppedDims = curTimeToAppendToDroppedDims;
    }

    public boolean restructureSchema(List<CubeDimension> newDimensions, List<Measure> newMeasures, Map<String, String> defaultValues, MolapDef.Schema origUnModifiedSchema, MolapDef.Schema schema, List<String> validDropDimList, List<String> validDropMsrList)
    {
        String prevRSFolderPathPrefix = pathTillRSFolderParent + File.separator + MolapCommonConstants.RESTRUCTRE_FOLDER;
        String sliceMetaDatapath = prevRSFolderPathPrefix  + currentRestructFolderNumber + File.separator + factTableName;
        
        SliceMetaData currentSliceMetaData = MolapUtil.readSliceMetaDataFile(sliceMetaDatapath, currentRestructFolderNumber);
        if (null == currentSliceMetaData)
        {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Failed to read current sliceMetaData from:"+sliceMetaDatapath);
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "May be dataloading is not done even once:"+sliceMetaDatapath);
            return true;
        }

        MolapDef.Cube origUnModifiedCube = MolapSchemaParser
                .getMondrianCube(origUnModifiedSchema, cubeName);

        if (!processDroppedDimsMsrs(prevRSFolderPathPrefix, currentRestructFolderNumber, validDropDimList, validDropMsrList,origUnModifiedCube))
        {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Failed to drop the dimension/measure");
            return false;
        }

        if (newDimensions.isEmpty() && newMeasures.isEmpty())
        {
            return true;
        }
    	
    	List<String> dimensions = new ArrayList<String>(Arrays.asList(currentSliceMetaData.getDimensions()));
    	List<String> dimsToAddToOldSliceMetaData = new ArrayList<String>();
        List<String> measures = new ArrayList<String>(Arrays.asList(currentSliceMetaData.getMeasures()));
        List<String> measureAggregators = new ArrayList<String>(Arrays.asList(currentSliceMetaData.getMeasuresAggregator()));
        Map<String, String> defValuesWithFactTableNames = new HashMap<String, String>();
    	
        
    	for (Measure aMeasure : newMeasures)
    	{
    		measures.add(aMeasure.column);
    		measureAggregators.add(aMeasure.aggregator);
    	}
    	
    	
    	
    	String tmpsliceMetaDataPath = prevRSFolderPathPrefix  + currentRestructFolderNumber + File.separator + factTableName;
    	int curLoadCounter = MolapUtil.checkAndReturnCurrentLoadFolderNumber(tmpsliceMetaDataPath);
    	
    	int newLoadCounter = curLoadCounter + 1;
    	
    	String newLevelFolderPath = newSliceMetaDataPath + File.separator
                + MolapCommonConstants.LOAD_FOLDER + newLoadCounter + File.separator;
    	
    	if (!createLoadFolder(newLevelFolderPath))
    	{
    	    LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Failed to create load folder:"+newLevelFolderPath);
    	    return false;
    	}
    	
        MolapDef.Cube cube = MolapSchemaParser
                .getMondrianCube(schema, cubeName);
        if(!createAggregateTableAfterRestructure(newLoadCounter, cube))
        {
            return false;
        }
    	
    	int[] currDimCardinality = null;
        
        try
        {
            currDimCardinality = readcurrentLevelCardinalityFile(
                    tmpsliceMetaDataPath + File.separator
                            + MolapCommonConstants.LOAD_FOLDER + curLoadCounter,
                    factTableName);
            if (null == currDimCardinality)
            {
                LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Level cardinality file is missing.Was empty load folder created to maintain load folder count in sync?");
            }
        }
        catch(MolapUtilException e)
        {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e.getMessage());
            return false;
        }
        
        List<Integer> dimLens = (null != currDimCardinality) ? new ArrayList<Integer>(Arrays.asList(ArrayUtils.toObject(currDimCardinality))) : new ArrayList<Integer>();

    	String defaultVal = null;
    	String levelColName;
        for(CubeDimension aDimension : newDimensions)
        {
            try
            {
                levelColName = ((MolapDef.Dimension)aDimension).hierarchies[0].levels[0].column;
                
                RelationOrJoin relation = ((MolapDef.Dimension)aDimension).hierarchies[0].relation;

                String tableName = relation == null ? factTableName
                        : ((Table)((MolapDef.Dimension)aDimension).hierarchies[0].relation).name;
                
                dimensions.add(tableName + '_' + levelColName);
                dimsToAddToOldSliceMetaData.add(tableName + '_' + levelColName);
                defaultVal = defaultValues.get(aDimension.name) == null ? null : defaultValues.get(aDimension.name);
                if (null != defaultVal)
                {
                    defValuesWithFactTableNames.put(tableName + '_' + levelColName, defaultVal);
                    dimLens.add(2);
                }
                else
                {
                    dimLens.add(1);
                }
                levelFilePrefix=tableName+'_';
                createLevelFiles(newLevelFolderPath, levelFilePrefix + ((MolapDef.Dimension)aDimension).hierarchies[0].levels[0].column
                        + MolapCommonConstants.LEVEL_FILE_EXTENSION,
                        defaultVal);
                LevelSortIndexWriterThread levelFileUpdater= new LevelSortIndexWriterThread(newLevelFolderPath+ levelFilePrefix + ((MolapDef.Dimension)aDimension).hierarchies[0].levels[0].column
                        + MolapCommonConstants.LEVEL_FILE_EXTENSION, ((MolapDef.Dimension)aDimension).hierarchies[0].levels[0].type);
                levelFileUpdater.call();
            }
            catch(IOException e)
            {
                return false;
            }
            catch(Exception e)
            {
            	return false;
            }
        }
        
        SliceMetaData newSliceMetaData = new SliceMetaData();
        
        newSliceMetaData.setDimensions(dimensions.toArray(new String[dimensions.size()]));
        newSliceMetaData.setActualDimensions(dimensions.toArray(new String[dimensions.size()]));
        newSliceMetaData.setMeasures(measures.toArray(new String[measures.size()]));
        newSliceMetaData.setTableNamesToLoadMandatory(null);
        newSliceMetaData.setMeasuresAggregator(measureAggregators.toArray(new String[measureAggregators.size()]));
        
        newSliceMetaData.setHeirAnKeySize(MolapSchemaParser.getHeirAndKeySizeMapForFact(cube.dimensions, schema));
        

    	int[] updatedCardinality = ArrayUtils.toPrimitive(dimLens.toArray(new Integer[dimLens.size()]));
    	try
        {
            writeLevelCardinalityFile(newLevelFolderPath, factTableName, updatedCardinality);
        }
        catch(KettleException e)
        {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e.getMessage());
            return false;
        }
    	
    	newSliceMetaData.setDimLens(updatedCardinality);
        newSliceMetaData.setActualDimLens(updatedCardinality);
        newSliceMetaData.setKeyGenerator(KeyGeneratorFactory.getKeyGenerator(newSliceMetaData.getDimLens()));
    	
    	MolapUtil.writeSliceMetaDataFile(newSliceMetaDataPath, newSliceMetaData, nextRestructFolder);
    	
    	SliceMetaData readSliceMetaDataFile = null;
    	
    	for(int folderNumber = currentRestructFolderNumber; folderNumber >= 0; folderNumber--)
        {
            sliceMetaDatapath = prevRSFolderPathPrefix  + folderNumber + File.separator + factTableName;
            readSliceMetaDataFile = MolapUtil.readSliceMetaDataFile(sliceMetaDatapath, currentRestructFolderNumber);
            if(null == readSliceMetaDataFile)
            {
                continue;
            }

            updateSliceMetadata(dimsToAddToOldSliceMetaData, newMeasures, defValuesWithFactTableNames, defaultValues, readSliceMetaDataFile, sliceMetaDatapath, newSliceMetaDataFileExtn);
            addNewSliceMetaDataForAggTables(folderNumber);
        }
    	
    	return true;
    }

    /**
     * We need to modify the slicemetadata with the dimension/measure being dropped.The dimension/measure can be a base dimension
     * or measure or it can be a newly added one which is being dropped
     * @param prevRSFolderPathPrefix
     * @param currentRestructFolderNumber
     * @param validDropDimList
     * @param validDropMsrList
     * @return
     */
    private boolean processDroppedDimsMsrs(String prevRSFolderPathPrefix, int currentRestructFolderNumber, List<String> validDropDimList, List<String> validDropMsrList,MolapDef.Cube cube)
    {
        if (0 == validDropDimList.size() && 0 == validDropMsrList.size())
        {
            return true;
        }

        SliceMetaData currentSliceMetaData = null;
        String sliceMetaDatapath = null;
        for(int folderNumber = currentRestructFolderNumber; folderNumber >= 0; folderNumber--)
        {
            sliceMetaDatapath = prevRSFolderPathPrefix  + folderNumber + File.separator + factTableName;
            currentSliceMetaData = MolapUtil.readSliceMetaDataFile(sliceMetaDatapath, currentRestructFolderNumber);
            if(null == currentSliceMetaData)
            {
                continue;
            }

            if (validDropMsrList.size() > 0)
            {
                String msrInSliceMeta = null;
                String newMsrInSliceMeta = null;
                for (String aMsr : validDropMsrList)
                {
                    //is the measure being dropped a base measure
                    if (null != currentSliceMetaData.getMeasures())
                    {
                        for (int msrIdx = 0; msrIdx < currentSliceMetaData.getMeasures().length; msrIdx++)
                        {
                            msrInSliceMeta = currentSliceMetaData.getMeasures()[msrIdx];
                            if (msrInSliceMeta.equals(aMsr))
                            {
                                currentSliceMetaData.getMeasures()[msrIdx] = msrInSliceMeta + '_' + this.curTimeToAppendToDroppedDims;
                            }
                        }
                    }

                    // is the measure being dropped a new measure
                    if (null != currentSliceMetaData.getNewMeasures())
                    {
                        for (int newMsrIdx = 0; newMsrIdx < currentSliceMetaData.getNewMeasures().length; newMsrIdx++)
                        {
                            newMsrInSliceMeta = currentSliceMetaData.getNewMeasures()[newMsrIdx];
                            if (newMsrInSliceMeta.equals(aMsr))
                            {
                                currentSliceMetaData.getNewMeasures()[newMsrIdx] = newMsrInSliceMeta + '_' + this.curTimeToAppendToDroppedDims;
                            }
                        }
                    }
                }
            }

            List<String> levelFilesToDelete = new ArrayList<String>();

            if (validDropDimList.size() > 0)
            {
                String dimInSliceMeta = null;
                String newDimInSliceMeta = null;
                Dimension schemaDim = null;
                for (String aDim : validDropDimList)
                {
                	 schemaDim = MolapSchemaParser.findDimension(cube.dimensions, aDim);
                	 if(null==schemaDim)
                	 {
                		 continue;
                	 }
                     RelationOrJoin relation = ((MolapDef.Dimension)schemaDim).hierarchies[0].relation;

                     String tableName = relation == null ? factTableName
                             : ((Table)((MolapDef.Dimension)schemaDim).hierarchies[0].relation).name;
                    //is the dimension being dropped a base dimension
                    if (null != currentSliceMetaData.getDimensions())
                    {
                        for (int dimIdx = 0; dimIdx < currentSliceMetaData.getDimensions().length; dimIdx++)
                        {
                            dimInSliceMeta = currentSliceMetaData.getDimensions()[dimIdx];
                           
                            if (dimInSliceMeta.equals(tableName + '_' + schemaDim.hierarchies[0].levels[0].column))
                            {
                                levelFilesToDelete.add(tableName + '_' + schemaDim.hierarchies[0].levels[0].column + ".level");
                                currentSliceMetaData.getDimensions()[dimIdx] = dimInSliceMeta + '_' + this.curTimeToAppendToDroppedDims;
                            }
                        }
                    }

                    //is the dimension being dropped a new dimension
                    if (null != currentSliceMetaData.getNewDimensions())
                    {
                        for (int newDimIdx = 0; newDimIdx < currentSliceMetaData.getNewDimensions().length; newDimIdx++)
                        {
                            newDimInSliceMeta = currentSliceMetaData.getNewDimensions()[newDimIdx];
                            if (newDimInSliceMeta.equals(tableName + '_' + aDim))
                            {
                                currentSliceMetaData.getNewDimensions()[newDimIdx] = newDimInSliceMeta + '_' + this.curTimeToAppendToDroppedDims;
                            }
                        }
                    }
                }
            }

            //for drop case no need to creare a new RS folder, overwrite the existing slicemetadata
            if (!overWriteSliceMetaDataFile(sliceMetaDatapath, currentSliceMetaData, currentRestructFolderNumber))
            {
                LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Failed to overwrite the slicemetadata in path:" + sliceMetaDatapath + " current RS is:" + currentRestructFolderNumber);
                return false;
            }

            if (levelFilesToDelete.size() > 0)
            {
                deleteDroppedDimsLevelFiles(levelFilesToDelete);
            }

        }


        return true;
    }

    private void deleteDroppedDimsLevelFiles(List<String> levelFilesToDelete)
    {
        String prevRSFolderPathPrefix = pathTillRSFolderParent + File.separator + MolapCommonConstants.RESTRUCTRE_FOLDER;
        String sliceMetaDatapath;
        String levelFilePath;
        MolapFile molapLevelFile = null;
        FileType molapFileType = FileFactory.getFileType(prevRSFolderPathPrefix);
        for(int folderNumber = currentRestructFolderNumber; folderNumber >= 0; folderNumber--)
        {
            sliceMetaDatapath = prevRSFolderPathPrefix  + folderNumber + File.separator + factTableName;
            MolapFile sliceMetaDataPathFolder = FileFactory.getMolapFile(sliceMetaDatapath, molapFileType);
            MolapFile[] loadFoldersArray = MolapUtil.listFiles(sliceMetaDataPathFolder);
            for (MolapFile aFile : loadFoldersArray)
            {
                for (String levelFileName : levelFilesToDelete)
                {
                    levelFilePath = aFile.getCanonicalPath() + File.separator + levelFileName;
                    try
                    {
                        if (FileFactory.isFileExist(levelFilePath, molapFileType))
                        {
                            MolapFile molapFile = FileFactory.getMolapFile(levelFilePath, molapFileType);
                            MolapUtil.deleteFoldersAndFiles(molapFile);
                        }
                    }
                    catch (IOException e)
                    {
                        LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Failed to delete level file:" + levelFileName + " in:" + aFile.getName());
                    }
                    catch (MolapUtilException e)
                    {
                        LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Failed to delete level file:" + levelFileName + " in:" + aFile.getName());
                    }
                }
            }
        }
    }


    /**
     * 
     * @param newLoadCounter
     * @param cube
     * 
     */
    private boolean createAggregateTableAfterRestructure(int newLoadCounter,
            MolapDef.Cube cube)
    {
        MolapDef.Table table = (MolapDef.Table)cube.fact;
        MolapDef.AggTable[] aggTables = table.aggTables;
        String aggTableName = null;
        String pathTillRSFolder = pathTillRSFolderParent + File.separator
                + newRSFolderName + File.separator;
        String aggTablePath = null;
        for(int i = 0;i < aggTables.length;i++)
        {
            aggTableName = ((MolapDef.AggName)aggTables[i]).getNameAttribute();
            aggTablePath = pathTillRSFolder + aggTableName + File.separator
                    + MolapCommonConstants.LOAD_FOLDER + newLoadCounter
                    + File.separator;
            if(!createLoadFolder(aggTablePath))
            {
                LOGGER.error(
                        MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Failed to create load folder for aggregate table in restructure :: "
                                + aggTablePath);
                return false;
            }
        }
        return true;
    }
    
    private boolean createLoadFolder(String newLevelFolderPath)
    {
        FileType fileType = FileFactory.getFileType(newLevelFolderPath);
        try
        {
            if (!FileFactory.isFileExist(newLevelFolderPath, fileType))
            {
                 return FileFactory.mkdirs(newLevelFolderPath, fileType);
            }
        }
        catch(IOException e)
        {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e.getMessage());
            return false;
        }
        return true;
    }

    private void addNewSliceMetaDataForAggTables(int rsFolderNumber)
    {
        String prevRSFolderPathPrefix = pathTillRSFolderParent + File.separator + MolapCommonConstants.RESTRUCTRE_FOLDER;
        String currentRSFolderPath = prevRSFolderPathPrefix  + rsFolderNumber + File.separator;
        MolapFile[] aggFolderList = null;
        String aggFolderSliceMetaDataPath = null;
        aggFolderList = getListOfAggTableFolders(currentRSFolderPath);
        
        if (null != aggFolderList && aggFolderList.length > 0)
        {
            for (MolapFile anAggFolder : aggFolderList)
            {
                aggFolderSliceMetaDataPath = currentRSFolderPath + anAggFolder.getName();
                makeCopyOfSliceMetaData(aggFolderSliceMetaDataPath, rsFolderNumber, nextRestructFolder);
            }
        }
    }
    
    private void makeCopyOfSliceMetaData(String aggFolderSliceMetaDataPath,
            int currRestructFolderNum, int nextRestructFolderNum)
    {
        SliceMetaData readSliceMetaDataFile = null;
        for(int i = currRestructFolderNum;i < nextRestructFolderNum;i++)
        {
            readSliceMetaDataFile = MolapUtil.readSliceMetaDataFile(
                    aggFolderSliceMetaDataPath, i);
            if(null == readSliceMetaDataFile)
            {
                continue;
            }
            MolapUtil.writeSliceMetaDataFile(aggFolderSliceMetaDataPath,
                    readSliceMetaDataFile, nextRestructFolderNum);
            break;
        }
    }

    private MolapFile[] getListOfAggTableFolders(String currentRSFolderPath)
    {
        MolapFile molapFile = FileFactory.getMolapFile(currentRSFolderPath,
                FileFactory.getFileType(currentRSFolderPath));

        // List of directories
        MolapFile[] listFolders = molapFile.listFiles(new MolapFileFilter()
        {
            @Override
            public boolean accept(MolapFile pathname)
            {
                if(pathname.isDirectory())
                {
                    if(pathname.getName().startsWith("agg_"))
                    {
                        return true;
                    }
                }
                return false;
            }
        });
        
        return listFolders;
    }
    
    private int[] readcurrentLevelCardinalityFile(String currentLoadFolderPath,
            String factTableName) throws MolapUtilException
    {
        int[] currDimCardinality = MolapUtil.getCardinalityFromLevelMetadataFile(currentLoadFolderPath + File.separator + MolapCommonConstants.LEVEL_METADATA_FILE + factTableName + ".metadata");
        return currDimCardinality;
        
    }

    private void updateSliceMetadata(List<String> newDimensions,
            List<Measure> newMeasures, Map<String, String> dimDefaultValues, Map<String, String> defaultValues,
            SliceMetaData oldSliceMetaData, String oldSliceMetaDatapath, String newSliceMetaDataFileExtn)
    {
        List<String> existingNewDimensions = (null != oldSliceMetaData.getNewDimensions()) ? new ArrayList<String>(Arrays
                .asList(oldSliceMetaData.getNewDimensions())) : new ArrayList<String>();
        
        List<Integer> existingNewDimLens = (null != oldSliceMetaData.getNewDimLens()) ? new ArrayList<Integer>(Arrays.asList(ArrayUtils
                .toObject(oldSliceMetaData.getNewDimLens()))) : new ArrayList<Integer>();
        
        List<Integer> existingNewDimsSurrogateKeys = (null != oldSliceMetaData.getNewDimsSurrogateKeys()) ? new ArrayList<Integer>(Arrays
                .asList(ArrayUtils.toObject(oldSliceMetaData.getNewDimsSurrogateKeys()))) : new ArrayList<Integer>();
        
        List<String> existingNewDimsDefVals = (null != oldSliceMetaData.getNewDimsDefVals()) ? new ArrayList<String>(Arrays
                .asList(oldSliceMetaData.getNewDimsDefVals())) : new ArrayList<String>();
                
        List<String> existingNewMeasures = (null != oldSliceMetaData.getNewMeasures()) ? new ArrayList<String>(Arrays.asList(oldSliceMetaData
                .getNewMeasures())) : new ArrayList<String>();
        
        List<Double> existingNewMeasureDftVals = (null != oldSliceMetaData.getNewMsrDfts()) ? new ArrayList<Double>(Arrays.asList(ArrayUtils
                .toObject(oldSliceMetaData.getNewMsrDfts()))) : new ArrayList<Double>();
        
        List<String> existingNewMeasureAggregators = (null != oldSliceMetaData.getNewMeasuresAggregator()) ? new ArrayList<String>(Arrays
                .asList(oldSliceMetaData.getNewMeasuresAggregator())) : new ArrayList<String>();

        existingNewDimensions.addAll(newDimensions);

        String dimDefVal;
        for(int i = 0;i < newDimensions.size();i++)
        {
            dimDefVal = dimDefaultValues.get(newDimensions.get(i));
            if(null == dimDefVal)
            {
                existingNewDimsDefVals.add(MolapCommonConstants.MEMBER_DEFAULT_VAL);
                existingNewDimsSurrogateKeys.add(DEF_SURROGATE_KEY);
                existingNewDimLens.add(1);
            }
            else
            {
                existingNewDimsDefVals.add(dimDefVal);
                existingNewDimsSurrogateKeys.add(DEF_SURROGATE_KEY + 1);
                existingNewDimLens.add(2);
            }
        }

        oldSliceMetaData.setNewDimLens(ArrayUtils.toPrimitive(existingNewDimLens.toArray(new Integer[existingNewDimLens
                .size()])));
        oldSliceMetaData.setNewActualDimLens(ArrayUtils.toPrimitive(existingNewDimLens
                .toArray(new Integer[existingNewDimLens.size()])));
        oldSliceMetaData.setNewDimensions(existingNewDimensions.toArray(new String[existingNewDimensions.size()]));
        oldSliceMetaData
                .setNewActualDimensions(existingNewDimensions.toArray(new String[existingNewDimensions.size()]));
        oldSliceMetaData.setNewDimsDefVals(existingNewDimsDefVals.toArray(new String[existingNewDimsDefVals.size()]));
        oldSliceMetaData.setNewDimsSurrogateKeys(ArrayUtils.toPrimitive(existingNewDimsSurrogateKeys
                .toArray(new Integer[existingNewDimsSurrogateKeys.size()])));

        String doubleVal;
        Double val;

        for(Measure aMeasure : newMeasures)
        {
            existingNewMeasures.add(aMeasure.column);
            doubleVal = defaultValues.get(aMeasure.name);
            if(null != doubleVal && 0 != doubleVal.trim().length())
            {
                try
                {
                    val = Double.parseDouble(doubleVal);
                    existingNewMeasureDftVals.add(val);
                    existingNewMeasureAggregators.add(aMeasure.aggregator);
                }
                catch(NumberFormatException e)
                {
                    existingNewMeasureDftVals.add(0.0);
                    existingNewMeasureAggregators.add(aMeasure.aggregator);
                }
                continue;
            }
            else
            {
                existingNewMeasureDftVals.add(0.0);
                existingNewMeasureAggregators.add(aMeasure.aggregator);
            }
        }

        oldSliceMetaData.setNewMeasures(existingNewMeasures.toArray(new String[existingNewMeasures.size()]));

        oldSliceMetaData.setNewMsrDfts(ArrayUtils.toPrimitive(existingNewMeasureDftVals
                .toArray(new Double[existingNewMeasureDftVals.size()])));
        oldSliceMetaData.setNewMeasuresAggregator(existingNewMeasureAggregators
                .toArray(new String[existingNewMeasureAggregators.size()]));

        MolapUtil.writeSliceMetaDataFile(oldSliceMetaDatapath, oldSliceMetaData, nextRestructFolder);
    }

    private void createLevelFiles(String levelFilePath, String levelFileName, String defaultValue) throws IOException
    {
        FileType fileType = FileFactory.getFileType(levelFilePath);
        
        OutputStream stream = null;
        ByteBuffer buffer=getMemberByteBufferWithoutDefaultValue(defaultValue);
        try
        {
            stream = FileFactory.getDataOutputStream(levelFilePath + levelFileName, fileType);
            stream.write(buffer.array());
        }
        catch(IOException e)
        {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e.getMessage());
            throw e;
        }
        finally
        {
            MolapUtil.closeStreams(stream);
        }
    }
    
    private static ByteBuffer getMemberByteBufferWithoutDefaultValue(String defaultValue)
    {
        int minValue=1;
        int rowLength=8;
        boolean enableEncoding = Boolean.valueOf(MolapProperties.getInstance().getProperty(
                MolapCommonConstants.ENABLE_BASE64_ENCODING,
                MolapCommonConstants.ENABLE_BASE64_ENCODING_DEFAULT));
        ByteBuffer buffer = null;
        byte[] data= null;
        if(enableEncoding)
        {
            try 
            {
                data = Base64.encodeBase64(MolapCommonConstants.MEMBER_DEFAULT_VAL.getBytes("UTF-8"));
            } 
            catch (UnsupportedEncodingException e) 
            {
                data =  Base64.encodeBase64(MolapCommonConstants.MEMBER_DEFAULT_VAL.getBytes());
            }
        }
        else
        {
            try
            {
                data=MolapCommonConstants.MEMBER_DEFAULT_VAL.getBytes("UTF-8");
            }
            catch(UnsupportedEncodingException e)
            {
                data=MolapCommonConstants.MEMBER_DEFAULT_VAL.getBytes();
            }
            
        }
        rowLength+=4;
        rowLength+=data.length;
        if(null==defaultValue)
        {
            buffer= ByteBuffer.allocate(rowLength);
            buffer.putInt(minValue);
            buffer.putInt(data.length);
            buffer.put(data);
            buffer.putInt(minValue);
        }
        else
        {
            byte[] data1= null;
            if(enableEncoding)
            {
                try 
                {
                    data1 = Base64.encodeBase64(defaultValue.getBytes("UTF-8"));
                } 
                catch (UnsupportedEncodingException e) 
                {
                    data1 =  Base64.encodeBase64(defaultValue.getBytes());
                }
            }
            else
            {
                try
                {
                    data1=defaultValue.getBytes("UTF-8");
                }
                catch(UnsupportedEncodingException e)
                {
                    data1=defaultValue.getBytes();
                }
                
            }
            rowLength+=4;
            rowLength+=data1.length;
            buffer= ByteBuffer.allocate(rowLength);
            buffer.putInt(minValue);
            buffer.putInt(data.length);
            buffer.put(data);
            buffer.putInt(data1.length);
            buffer.put(data1);
            buffer.putInt(2);
        }
        buffer.flip();
        return buffer;
    }

    private void writeLevelCardinalityFile(String loadFolderLoc,
            String tableName, int[] dimCardinality) throws KettleException
    {
        String levelCardinalityFilePath = loadFolderLoc + File.separator
                + MolapCommonConstants.LEVEL_METADATA_FILE + tableName
                + ".metadata";

        DataOutputStream outstream = null;
        try
        {
            int dimCardinalityArrLength = dimCardinality.length;

            outstream = FileFactory.getDataOutputStream(
                    levelCardinalityFilePath,
                    FileFactory.getFileType(levelCardinalityFilePath));
            outstream.writeInt(dimCardinalityArrLength);

            for(int i = 0;i < dimCardinalityArrLength;i++)
            {
                outstream.writeInt(dimCardinality[i]);
            }

            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "Level cardinality file written to : "
                            + levelCardinalityFilePath);
        }
        catch(IOException e)
        {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "Error while writing level cardinality file : "
                            + levelCardinalityFilePath + e.getMessage());
            throw new KettleException(
                    "Not able to write level cardinality file", e);
        }
        finally
        {
            MolapUtil.closeStreams(outstream);
        }
    }

    /**
     * overwrite the existing slicemetadata.
     * @param path
     * @param sliceMetaData
     * @param restructFolder
     * @return
     */
    private boolean overWriteSliceMetaDataFile(String path, SliceMetaData sliceMetaData, int restructFolder)
    {
        //file name to write the slicemetadata before moving
        String tmpSliceMetaDataFileName = path + File.separator + MolapUtil.getSliceMetaDataFileName(restructFolder) + ".tmp";
        String presentSliceMetaDataFileName = path + File.separator + MolapUtil.getSliceMetaDataFileName(restructFolder);

        FileType fileType = FileFactory.getFileType(tmpSliceMetaDataFileName);

        OutputStream stream = null;
        ObjectOutputStream objectOutputStream = null;
        boolean createSuccess = true;
        try
        {
            //if tmp slicemetadata is present, that means cleanup was not correct, delete it
            if(FileFactory.isFileExist(tmpSliceMetaDataFileName, fileType))
            {
                FileFactory.getMolapFile(tmpSliceMetaDataFileName, fileType).delete();
            }
            //write the updated slicemetadata to tmp file first
            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "Slice Metadata file Path: " + path + '/'
                    + MolapUtil.getSliceMetaDataFileName(restructFolder));
            stream = FileFactory.getDataOutputStream(tmpSliceMetaDataFileName, FileFactory.getFileType(path));
            objectOutputStream = new ObjectOutputStream(stream);
            objectOutputStream.writeObject(sliceMetaData);
        }
        catch(IOException e)
        {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e.getMessage());
            createSuccess = false;

        }
        finally
        {
            MolapUtil.closeStreams(objectOutputStream, stream);
            if (createSuccess)
            {
                //if tmp slicemetadata creation is success, rename it to actual slicemetadata name
                MolapFile file = FileFactory.getMolapFile(tmpSliceMetaDataFileName, fileType);
                return file.renameForce(presentSliceMetaDataFileName);
            }
        }

        return createSuccess;
    }
}
