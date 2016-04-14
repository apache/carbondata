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

package org.carbondata.processing.restructure;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.ArrayUtils;
import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.carbondata.core.metadata.SliceMetaData;
import org.carbondata.core.carbon.CarbonDef;
import org.carbondata.core.carbon.CarbonDef.*;
import org.carbondata.core.util.CarbonCoreLogEvent;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.util.CarbonUtilException;
import org.carbondata.processing.util.CarbonDataProcessorLogEvent;
import org.carbondata.processing.util.LevelSortIndexWriterThread;
import org.carbondata.processing.util.CarbonSchemaParser;
import org.pentaho.di.core.exception.KettleException;

public class SchemaRestructurer {
    /**
     * Comment for <code>LOGGER</code>
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(SchemaRestructurer.class.getName());

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

    public SchemaRestructurer(String schemaName, String cubeName, String factTableName,
            String hdfsStoreLocation, int currentRestructFolderNum,
            long curTimeToAppendToDroppedDims) {
        pathTillRSFolderParent =
                hdfsStoreLocation + File.separator + schemaName + File.separator + cubeName;
        this.currentRestructFolderNumber = currentRestructFolderNum;
        if (-1 == currentRestructFolderNumber) {
            currentRestructFolderNumber = 0;
        }
        nextRestructFolder = currentRestructFolderNumber + 1;

        newRSFolderName = CarbonCommonConstants.RESTRUCTRE_FOLDER + nextRestructFolder;

        newSliceMetaDataPath =
                pathTillRSFolderParent + File.separator + newRSFolderName + File.separator
                        + factTableName;

        this.factTableName = factTableName;
        this.cubeName = cubeName.substring(0, cubeName.lastIndexOf('_'));
        this.curTimeToAppendToDroppedDims = curTimeToAppendToDroppedDims;
    }

    private static ByteBuffer getMemberByteBufferWithoutDefaultValue(String defaultValue) {
        int minValue = 1;
        int rowLength = 8;
        boolean enableEncoding = Boolean.valueOf(CarbonProperties.getInstance()
                .getProperty(CarbonCommonConstants.ENABLE_BASE64_ENCODING,
                        CarbonCommonConstants.ENABLE_BASE64_ENCODING_DEFAULT));
        ByteBuffer buffer = null;
        byte[] data = null;
        if (enableEncoding) {
            try {
                data = Base64.encodeBase64(
                        CarbonCommonConstants.MEMBER_DEFAULT_VAL.getBytes("UTF-8"));
            } catch (UnsupportedEncodingException e) {
                data = Base64.encodeBase64(CarbonCommonConstants.MEMBER_DEFAULT_VAL.getBytes());
            }
        } else {
            try {
                data = CarbonCommonConstants.MEMBER_DEFAULT_VAL.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                data = CarbonCommonConstants.MEMBER_DEFAULT_VAL.getBytes();
            }

        }
        rowLength += 4;
        rowLength += data.length;
        if (null == defaultValue) {
            buffer = ByteBuffer.allocate(rowLength);
            buffer.putInt(minValue);
            buffer.putInt(data.length);
            buffer.put(data);
            buffer.putInt(minValue);
        } else {
            byte[] data1 = null;
            if (enableEncoding) {
                try {
                    data1 = Base64.encodeBase64(defaultValue.getBytes("UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    data1 = Base64.encodeBase64(defaultValue.getBytes());
                }
            } else {
                try {
                    data1 = defaultValue.getBytes("UTF-8");
                } catch (UnsupportedEncodingException e) {
                    data1 = defaultValue.getBytes();
                }

            }
            rowLength += 4;
            rowLength += data1.length;
            buffer = ByteBuffer.allocate(rowLength);
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

    public boolean restructureSchema(List<CubeDimension> newDimensions, List<Measure> newMeasures,
            Map<String, String> defaultValues, CarbonDef.Schema origUnModifiedSchema,
            CarbonDef.Schema schema, List<String> validDropDimList, List<String> validDropMsrList) {
        String prevRSFolderPathPrefix =
                pathTillRSFolderParent + File.separator + CarbonCommonConstants.RESTRUCTRE_FOLDER;
        String sliceMetaDatapath =
                prevRSFolderPathPrefix + currentRestructFolderNumber + File.separator
                        + factTableName;

        SliceMetaData currentSliceMetaData =
                CarbonUtil.readSliceMetaDataFile(sliceMetaDatapath, currentRestructFolderNumber);
        if (null == currentSliceMetaData) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                    "Failed to read current sliceMetaData from:" + sliceMetaDatapath);
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                    "May be dataloading is not done even once:" + sliceMetaDatapath);
            return true;
        }

        CarbonDef.Cube origUnModifiedCube =
                CarbonSchemaParser.getMondrianCube(origUnModifiedSchema, cubeName);

        if (!processDroppedDimsMsrs(prevRSFolderPathPrefix, currentRestructFolderNumber,
                validDropDimList, validDropMsrList, origUnModifiedCube)) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                    "Failed to drop the dimension/measure");
            return false;
        }

        if (newDimensions.isEmpty() && newMeasures.isEmpty()) {
            return true;
        }

        List<String> dimensions =
                new ArrayList<String>(Arrays.asList(currentSliceMetaData.getDimensions()));
        List<String> dimsToAddToOldSliceMetaData = new ArrayList<String>();
        List<String> measures =
                new ArrayList<String>(Arrays.asList(currentSliceMetaData.getMeasures()));
        List<String> measureAggregators =
                new ArrayList<String>(Arrays.asList(currentSliceMetaData.getMeasuresAggregator()));
        Map<String, String> defValuesWithFactTableNames = new HashMap<String, String>();

        for (Measure aMeasure : newMeasures) {
            measures.add(aMeasure.column);
            measureAggregators.add(aMeasure.aggregator);
        }

        String tmpsliceMetaDataPath =
                prevRSFolderPathPrefix + currentRestructFolderNumber + File.separator
                        + factTableName;
        int curLoadCounter = CarbonUtil.checkAndReturnCurrentLoadFolderNumber(tmpsliceMetaDataPath);

        int newLoadCounter = curLoadCounter + 1;

        String newLevelFolderPath =
                newSliceMetaDataPath + File.separator + CarbonCommonConstants.LOAD_FOLDER
                        + newLoadCounter + File.separator;

        if (!createLoadFolder(newLevelFolderPath)) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                    "Failed to create load folder:" + newLevelFolderPath);
            return false;
        }

        CarbonDef.Cube cube = CarbonSchemaParser.getMondrianCube(schema, cubeName);
        if (!createAggregateTableAfterRestructure(newLoadCounter, cube)) {
            return false;
        }

        int[] currDimCardinality = null;

        try {
            currDimCardinality = readcurrentLevelCardinalityFile(
                    tmpsliceMetaDataPath + File.separator + CarbonCommonConstants.LOAD_FOLDER
                            + curLoadCounter, factTableName);
            if (null == currDimCardinality) {
                LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                        "Level cardinality file is missing.Was empty load folder created to maintain load folder count in sync?");
            }
        } catch (CarbonUtilException e) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, e.getMessage());
            return false;
        }

        List<Integer> dimLens = (null != currDimCardinality) ?
                new ArrayList<Integer>(Arrays.asList(ArrayUtils.toObject(currDimCardinality))) :
                new ArrayList<Integer>();

        String defaultVal = null;
        String levelColName;
        for (CubeDimension aDimension : newDimensions) {
            try {
                levelColName = ((CarbonDef.Dimension) aDimension).hierarchies[0].levels[0].column;

                RelationOrJoin relation = ((CarbonDef.Dimension) aDimension).hierarchies[0].relation;

                String tableName = relation == null ?
                        factTableName :
                        ((Table) ((CarbonDef.Dimension) aDimension).hierarchies[0].relation).name;

                dimensions.add(tableName + '_' + levelColName);
                dimsToAddToOldSliceMetaData.add(tableName + '_' + levelColName);
                defaultVal = defaultValues.get(aDimension.name) == null ?
                        null :
                        defaultValues.get(aDimension.name);
                if(aDimension.noDictionary)
                {
                	continue;
                }
                if (null != defaultVal) {
                    defValuesWithFactTableNames.put(tableName + '_' + levelColName, defaultVal);
                    dimLens.add(2);
                } else {
                    dimLens.add(1);
                }
                levelFilePrefix = tableName + '_';
                createLevelFiles(newLevelFolderPath, levelFilePrefix
                        + ((CarbonDef.Dimension) aDimension).hierarchies[0].levels[0].column
                        + CarbonCommonConstants.LEVEL_FILE_EXTENSION, defaultVal);
                LevelSortIndexWriterThread levelFileUpdater = new LevelSortIndexWriterThread(
                        newLevelFolderPath + levelFilePrefix
                                + ((CarbonDef.Dimension) aDimension).hierarchies[0].levels[0].column
                                + CarbonCommonConstants.LEVEL_FILE_EXTENSION,
                        ((CarbonDef.Dimension) aDimension).hierarchies[0].levels[0].type);
                levelFileUpdater.call();
            } catch (IOException e) {
                return false;
            } catch (Exception e) {
                return false;
            }
        }

        SliceMetaData newSliceMetaData = new SliceMetaData();

        newSliceMetaData.setDimensions(dimensions.toArray(new String[dimensions.size()]));
        newSliceMetaData.setActualDimensions(dimensions.toArray(new String[dimensions.size()]));
        newSliceMetaData.setMeasures(measures.toArray(new String[measures.size()]));
        newSliceMetaData.setTableNamesToLoadMandatory(null);
        newSliceMetaData.setMeasuresAggregator(
                measureAggregators.toArray(new String[measureAggregators.size()]));

        newSliceMetaData.setHeirAnKeySize(
                CarbonSchemaParser.getHeirAndKeySizeMapForFact(cube.dimensions, schema));

        int[] updatedCardinality =
                ArrayUtils.toPrimitive(dimLens.toArray(new Integer[dimLens.size()]));
        try {
            writeLevelCardinalityFile(newLevelFolderPath, factTableName, updatedCardinality);
        } catch (KettleException e) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, e.getMessage());
            return false;
        }

        newSliceMetaData.setDimLens(updatedCardinality);
        newSliceMetaData.setActualDimLens(updatedCardinality);
        newSliceMetaData.setKeyGenerator(
                KeyGeneratorFactory.getKeyGenerator(newSliceMetaData.getDimLens()));

        CarbonUtil
                .writeSliceMetaDataFile(newSliceMetaDataPath, newSliceMetaData, nextRestructFolder);

        SliceMetaData readSliceMetaDataFile = null;

        for (int folderNumber = currentRestructFolderNumber; folderNumber >= 0; folderNumber--) {
            sliceMetaDatapath =
                    prevRSFolderPathPrefix + folderNumber + File.separator + factTableName;
            readSliceMetaDataFile =
                    CarbonUtil.readSliceMetaDataFile(sliceMetaDatapath, currentRestructFolderNumber);
            if (null == readSliceMetaDataFile) {
                continue;
            }

            updateSliceMetadata(dimsToAddToOldSliceMetaData, newMeasures,
                    defValuesWithFactTableNames, defaultValues, readSliceMetaDataFile,
                    sliceMetaDatapath, newSliceMetaDataFileExtn);
            addNewSliceMetaDataForAggTables(folderNumber);
        }

        return true;
    }

    /**
     * We need to modify the slicemetadata with the dimension/measure being dropped.The dimension/measure can be a base dimension
     * or measure or it can be a newly added one which is being dropped
     *
     * @param prevRSFolderPathPrefix
     * @param currentRestructFolderNumber
     * @param validDropDimList
     * @param validDropMsrList
     * @return
     */
    private boolean processDroppedDimsMsrs(String prevRSFolderPathPrefix,
            int currentRestructFolderNumber, List<String> validDropDimList,
            List<String> validDropMsrList, CarbonDef.Cube cube) {
        if (0 == validDropDimList.size() && 0 == validDropMsrList.size()) {
            return true;
        }

        SliceMetaData currentSliceMetaData = null;
        String sliceMetaDatapath = null;
        for (int folderNumber = currentRestructFolderNumber; folderNumber >= 0; folderNumber--) {
            sliceMetaDatapath =
                    prevRSFolderPathPrefix + folderNumber + File.separator + factTableName;
            currentSliceMetaData =
                    CarbonUtil.readSliceMetaDataFile(sliceMetaDatapath, currentRestructFolderNumber);
            if (null == currentSliceMetaData) {
                continue;
            }

            if (validDropMsrList.size() > 0) {
                String msrInSliceMeta = null;
                String newMsrInSliceMeta = null;
                for (String aMsr : validDropMsrList) {
                    //is the measure being dropped a base measure
                    if (null != currentSliceMetaData.getMeasures()) {
                        for (int msrIdx = 0;
                             msrIdx < currentSliceMetaData.getMeasures().length; msrIdx++) {
                            msrInSliceMeta = currentSliceMetaData.getMeasures()[msrIdx];
                            if (msrInSliceMeta.equals(aMsr)) {
                                currentSliceMetaData.getMeasures()[msrIdx] =
                                        msrInSliceMeta + '_' + this.curTimeToAppendToDroppedDims;
                            }
                        }
                    }

                    // is the measure being dropped a new measure
                    if (null != currentSliceMetaData.getNewMeasures()) {
                        for (int newMsrIdx = 0; newMsrIdx < currentSliceMetaData
                                .getNewMeasures().length; newMsrIdx++) {
                            newMsrInSliceMeta = currentSliceMetaData.getNewMeasures()[newMsrIdx];
                            if (newMsrInSliceMeta.equals(aMsr)) {
                                currentSliceMetaData.getNewMeasures()[newMsrIdx] =
                                        newMsrInSliceMeta + '_' + this.curTimeToAppendToDroppedDims;
                            }
                        }
                    }
                }
            }

            List<String> levelFilesToDelete = new ArrayList<String>();

            if (validDropDimList.size() > 0) {
                String dimInSliceMeta = null;
                String newDimInSliceMeta = null;
                Dimension schemaDim = null;
                for (String aDim : validDropDimList) {
                    schemaDim = CarbonSchemaParser.findDimension(cube.dimensions, aDim);
                    if (null == schemaDim) {
                        continue;
                    }
                    RelationOrJoin relation =
                            ((CarbonDef.Dimension) schemaDim).hierarchies[0].relation;

                    String tableName = relation == null ?
                            factTableName :
                            ((Table) ((CarbonDef.Dimension) schemaDim).hierarchies[0].relation).name;
                    //is the dimension being dropped a base dimension
                    if (null != currentSliceMetaData.getDimensions()) {
                        for (int dimIdx = 0;
                             dimIdx < currentSliceMetaData.getDimensions().length; dimIdx++) {
                            dimInSliceMeta = currentSliceMetaData.getDimensions()[dimIdx];

                            if (dimInSliceMeta.equals(tableName + '_'
                                    + schemaDim.hierarchies[0].levels[0].column)) {
                                levelFilesToDelete.add(tableName + '_'
                                        + schemaDim.hierarchies[0].levels[0].column + ".level");
                                currentSliceMetaData.getDimensions()[dimIdx] =
                                        dimInSliceMeta + '_' + this.curTimeToAppendToDroppedDims;
                            }
                        }
                    }

                    //is the dimension being dropped a new dimension
                    if (null != currentSliceMetaData.getNewDimensions()) {
                        for (int newDimIdx = 0; newDimIdx < currentSliceMetaData
                                .getNewDimensions().length; newDimIdx++) {
                            newDimInSliceMeta = currentSliceMetaData.getNewDimensions()[newDimIdx];
                            if (newDimInSliceMeta.equals(tableName + '_' + aDim)) {
                                currentSliceMetaData.getNewDimensions()[newDimIdx] =
                                        newDimInSliceMeta + '_' + this.curTimeToAppendToDroppedDims;
                            }
                        }
                    }
                }
            }

            //for drop case no need to creare a new RS folder, overwrite the existing slicemetadata
            if (!overWriteSliceMetaDataFile(sliceMetaDatapath, currentSliceMetaData,
                    currentRestructFolderNumber)) {
                LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                        "Failed to overwrite the slicemetadata in path:" + sliceMetaDatapath
                                + " current RS is:" + currentRestructFolderNumber);
                return false;
            }

            if (levelFilesToDelete.size() > 0) {
                deleteDroppedDimsLevelFiles(levelFilesToDelete);
            }

        }

        return true;
    }

    private void deleteDroppedDimsLevelFiles(List<String> levelFilesToDelete) {
        String prevRSFolderPathPrefix =
                pathTillRSFolderParent + File.separator + CarbonCommonConstants.RESTRUCTRE_FOLDER;
        String sliceMetaDatapath;
        String levelFilePath;
        CarbonFile carbonLevelFile = null;
        FileType carbonFileType = FileFactory.getFileType(prevRSFolderPathPrefix);
        for (int folderNumber = currentRestructFolderNumber; folderNumber >= 0; folderNumber--) {
            sliceMetaDatapath =
                    prevRSFolderPathPrefix + folderNumber + File.separator + factTableName;
            CarbonFile sliceMetaDataPathFolder =
                    FileFactory.getCarbonFile(sliceMetaDatapath, carbonFileType);
            CarbonFile[] loadFoldersArray = CarbonUtil.listFiles(sliceMetaDataPathFolder);
            for (CarbonFile aFile : loadFoldersArray) {
                for (String levelFileName : levelFilesToDelete) {
                    levelFilePath = aFile.getCanonicalPath() + File.separator + levelFileName;
                    try {
                        if (FileFactory.isFileExist(levelFilePath, carbonFileType)) {
                            CarbonFile carbonFile =
                                    FileFactory.getCarbonFile(levelFilePath, carbonFileType);
                            CarbonUtil.deleteFoldersAndFiles(carbonFile);
                        }
                    } catch (IOException e) {
                        LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                                "Failed to delete level file:" + levelFileName + " in:" + aFile
                                        .getName());
                    } catch (CarbonUtilException e) {
                        LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                                "Failed to delete level file:" + levelFileName + " in:" + aFile
                                        .getName());
                    }
                }
            }
        }
    }

    /**
     * @param newLoadCounter
     * @param cube
     */
    private boolean createAggregateTableAfterRestructure(int newLoadCounter, CarbonDef.Cube cube) {
        CarbonDef.Table table = (CarbonDef.Table) cube.fact;
        CarbonDef.AggTable[] aggTables = table.aggTables;
        String aggTableName = null;
        String pathTillRSFolder =
                pathTillRSFolderParent + File.separator + newRSFolderName + File.separator;
        String aggTablePath = null;
        for (int i = 0; i < aggTables.length; i++) {
            aggTableName = ((CarbonDef.AggName) aggTables[i]).getNameAttribute();
            aggTablePath = pathTillRSFolder + aggTableName + File.separator
                    + CarbonCommonConstants.LOAD_FOLDER + newLoadCounter + File.separator;
            if (!createLoadFolder(aggTablePath)) {
                LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                        "Failed to create load folder for aggregate table in restructure :: "
                                + aggTablePath);
                return false;
            }
        }
        return true;
    }

    private boolean createLoadFolder(String newLevelFolderPath) {
        FileType fileType = FileFactory.getFileType(newLevelFolderPath);
        try {
            if (!FileFactory.isFileExist(newLevelFolderPath, fileType)) {
                return FileFactory.mkdirs(newLevelFolderPath, fileType);
            }
        } catch (IOException e) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, e.getMessage());
            return false;
        }
        return true;
    }

    private void addNewSliceMetaDataForAggTables(int rsFolderNumber) {
        String prevRSFolderPathPrefix =
                pathTillRSFolderParent + File.separator + CarbonCommonConstants.RESTRUCTRE_FOLDER;
        String currentRSFolderPath = prevRSFolderPathPrefix + rsFolderNumber + File.separator;
        CarbonFile[] aggFolderList = null;
        String aggFolderSliceMetaDataPath = null;
        aggFolderList = getListOfAggTableFolders(currentRSFolderPath);

        if (null != aggFolderList && aggFolderList.length > 0) {
            for (CarbonFile anAggFolder : aggFolderList) {
                aggFolderSliceMetaDataPath = currentRSFolderPath + anAggFolder.getName();
                makeCopyOfSliceMetaData(aggFolderSliceMetaDataPath, rsFolderNumber,
                        nextRestructFolder);
            }
        }
    }

    private void makeCopyOfSliceMetaData(String aggFolderSliceMetaDataPath,
            int currRestructFolderNum, int nextRestructFolderNum) {
        SliceMetaData readSliceMetaDataFile = null;
        for (int i = currRestructFolderNum; i < nextRestructFolderNum; i++) {
            readSliceMetaDataFile = CarbonUtil.readSliceMetaDataFile(aggFolderSliceMetaDataPath, i);
            if (null == readSliceMetaDataFile) {
                continue;
            }
            CarbonUtil.writeSliceMetaDataFile(aggFolderSliceMetaDataPath, readSliceMetaDataFile,
                    nextRestructFolderNum);
            break;
        }
    }

    private CarbonFile[] getListOfAggTableFolders(String currentRSFolderPath) {
        CarbonFile carbonFile = FileFactory
                .getCarbonFile(currentRSFolderPath, FileFactory.getFileType(currentRSFolderPath));

        // List of directories
        CarbonFile[] listFolders = carbonFile.listFiles(new CarbonFileFilter() {
            @Override
            public boolean accept(CarbonFile pathname) {
                if (pathname.isDirectory()) {
                    if (pathname.getName().startsWith("agg_")) {
                        return true;
                    }
                }
                return false;
            }
        });

        return listFolders;
    }

    private int[] readcurrentLevelCardinalityFile(String currentLoadFolderPath,
            String factTableName) throws CarbonUtilException {
        int[] currDimCardinality = CarbonUtil.getCardinalityFromLevelMetadataFile(
                currentLoadFolderPath + File.separator + CarbonCommonConstants.LEVEL_METADATA_FILE
                        + factTableName + ".metadata");
        return currDimCardinality;

    }

    private void updateSliceMetadata(List<String> newDimensions, List<Measure> newMeasures,
            Map<String, String> dimDefaultValues, Map<String, String> defaultValues,
            SliceMetaData oldSliceMetaData, String oldSliceMetaDatapath,
            String newSliceMetaDataFileExtn) {
        List<String> existingNewDimensions = (null != oldSliceMetaData.getNewDimensions()) ?
                new ArrayList<String>(Arrays.asList(oldSliceMetaData.getNewDimensions())) :
                new ArrayList<String>();

        List<Integer> existingNewDimLens = (null != oldSliceMetaData.getNewDimLens()) ?
                new ArrayList<Integer>(
                        Arrays.asList(ArrayUtils.toObject(oldSliceMetaData.getNewDimLens()))) :
                new ArrayList<Integer>();

        List<Integer> existingNewDimsSurrogateKeys =
                (null != oldSliceMetaData.getNewDimsSurrogateKeys()) ?
                        new ArrayList<Integer>(Arrays.asList(
                                ArrayUtils.toObject(oldSliceMetaData.getNewDimsSurrogateKeys()))) :
                        new ArrayList<Integer>();

        List<String> existingNewDimsDefVals = (null != oldSliceMetaData.getNewDimsDefVals()) ?
                new ArrayList<String>(Arrays.asList(oldSliceMetaData.getNewDimsDefVals())) :
                new ArrayList<String>();

        List<String> existingNewMeasures = (null != oldSliceMetaData.getNewMeasures()) ?
                new ArrayList<String>(Arrays.asList(oldSliceMetaData.getNewMeasures())) :
                new ArrayList<String>();

        List<Double> existingNewMeasureDftVals = (null != oldSliceMetaData.getNewMsrDfts()) ?
                new ArrayList<Double>(
                        Arrays.asList(ArrayUtils.toObject(oldSliceMetaData.getNewMsrDfts()))) :
                new ArrayList<Double>();

        List<String> existingNewMeasureAggregators =
                (null != oldSliceMetaData.getNewMeasuresAggregator()) ?
                        new ArrayList<String>(
                                Arrays.asList(oldSliceMetaData.getNewMeasuresAggregator())) :
                        new ArrayList<String>();

        existingNewDimensions.addAll(newDimensions);

        String dimDefVal;
        for (int i = 0; i < newDimensions.size(); i++) {
            dimDefVal = dimDefaultValues.get(newDimensions.get(i));
            if (null == dimDefVal) {
                existingNewDimsDefVals.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
                existingNewDimsSurrogateKeys.add(DEF_SURROGATE_KEY);
                existingNewDimLens.add(1);
            } else {
                existingNewDimsDefVals.add(dimDefVal);
                existingNewDimsSurrogateKeys.add(DEF_SURROGATE_KEY + 1);
                existingNewDimLens.add(2);
            }
        }

        oldSliceMetaData.setNewDimLens(ArrayUtils
                .toPrimitive(existingNewDimLens.toArray(new Integer[existingNewDimLens.size()])));
        oldSliceMetaData.setNewActualDimLens(ArrayUtils
                .toPrimitive(existingNewDimLens.toArray(new Integer[existingNewDimLens.size()])));
        oldSliceMetaData.setNewDimensions(
                existingNewDimensions.toArray(new String[existingNewDimensions.size()]));
        oldSliceMetaData.setNewActualDimensions(
                existingNewDimensions.toArray(new String[existingNewDimensions.size()]));
        oldSliceMetaData.setNewDimsDefVals(
                existingNewDimsDefVals.toArray(new String[existingNewDimsDefVals.size()]));
        oldSliceMetaData.setNewDimsSurrogateKeys(ArrayUtils.toPrimitive(existingNewDimsSurrogateKeys
                .toArray(new Integer[existingNewDimsSurrogateKeys.size()])));

        String doubleVal;
        Double val;

        for (Measure aMeasure : newMeasures) {
            existingNewMeasures.add(aMeasure.column);
            doubleVal = defaultValues.get(aMeasure.name);
            if (null != doubleVal && 0 != doubleVal.trim().length()) {
                try {
                    val = Double.parseDouble(doubleVal);
                    existingNewMeasureDftVals.add(val);
                    existingNewMeasureAggregators.add(aMeasure.aggregator);
                } catch (NumberFormatException e) {
                    existingNewMeasureDftVals.add(0.0);
                    existingNewMeasureAggregators.add(aMeasure.aggregator);
                }
                continue;
            } else {
                existingNewMeasureDftVals.add(0.0);
                existingNewMeasureAggregators.add(aMeasure.aggregator);
            }
        }

        oldSliceMetaData.setNewMeasures(
                existingNewMeasures.toArray(new String[existingNewMeasures.size()]));

        oldSliceMetaData.setNewMsrDfts(ArrayUtils.toPrimitive(
                existingNewMeasureDftVals.toArray(new Double[existingNewMeasureDftVals.size()])));
        oldSliceMetaData.setNewMeasuresAggregator(existingNewMeasureAggregators
                .toArray(new String[existingNewMeasureAggregators.size()]));

        CarbonUtil
                .writeSliceMetaDataFile(oldSliceMetaDatapath, oldSliceMetaData, nextRestructFolder);
    }

    private void createLevelFiles(String levelFilePath, String levelFileName, String defaultValue)
            throws IOException {
        FileType fileType = FileFactory.getFileType(levelFilePath);

        OutputStream stream = null;
        ByteBuffer buffer = getMemberByteBufferWithoutDefaultValue(defaultValue);
        try {
            stream = FileFactory.getDataOutputStream(levelFilePath + levelFileName, fileType);
            stream.write(buffer.array());
        } catch (IOException e) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, e.getMessage());
            throw e;
        } finally {
            CarbonUtil.closeStreams(stream);
        }
    }

    private void writeLevelCardinalityFile(String loadFolderLoc, String tableName,
            int[] dimCardinality) throws KettleException {
        String levelCardinalityFilePath =
                loadFolderLoc + File.separator + CarbonCommonConstants.LEVEL_METADATA_FILE
                        + tableName + ".metadata";

        DataOutputStream outstream = null;
        try {
            int dimCardinalityArrLength = dimCardinality.length;

            outstream = FileFactory.getDataOutputStream(levelCardinalityFilePath,
                    FileFactory.getFileType(levelCardinalityFilePath));
            outstream.writeInt(dimCardinalityArrLength);

            for (int i = 0; i < dimCardinalityArrLength; i++) {
                outstream.writeInt(dimCardinality[i]);
            }

            LOGGER.info(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
                    "Level cardinality file written to : " + levelCardinalityFilePath);
        } catch (IOException e) {
            LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
                    "Error while writing level cardinality file : " + levelCardinalityFilePath + e
                            .getMessage());
            throw new KettleException("Not able to write level cardinality file", e);
        } finally {
            CarbonUtil.closeStreams(outstream);
        }
    }

    /**
     * overwrite the existing slicemetadata.
     *
     * @param path
     * @param sliceMetaData
     * @param restructFolder
     * @return
     */
    private boolean overWriteSliceMetaDataFile(String path, SliceMetaData sliceMetaData,
            int restructFolder) {
        //file name to write the slicemetadata before moving
        String tmpSliceMetaDataFileName =
                path + File.separator + CarbonUtil.getSliceMetaDataFileName(restructFolder) + ".tmp";
        String presentSliceMetaDataFileName =
                path + File.separator + CarbonUtil.getSliceMetaDataFileName(restructFolder);

        FileType fileType = FileFactory.getFileType(tmpSliceMetaDataFileName);

        OutputStream stream = null;
        ObjectOutputStream objectOutputStream = null;
        boolean createSuccess = true;
        try {
            //if tmp slicemetadata is present, that means cleanup was not correct, delete it
            if (FileFactory.isFileExist(tmpSliceMetaDataFileName, fileType)) {
                FileFactory.getCarbonFile(tmpSliceMetaDataFileName, fileType).delete();
            }
            //write the updated slicemetadata to tmp file first
            LOGGER.info(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
                    "Slice Metadata file Path: " + path + '/' + CarbonUtil
                            .getSliceMetaDataFileName(restructFolder));
            stream = FileFactory
                    .getDataOutputStream(tmpSliceMetaDataFileName, FileFactory.getFileType(path));
            objectOutputStream = new ObjectOutputStream(stream);
            objectOutputStream.writeObject(sliceMetaData);
        } catch (IOException e) {
            LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG, e.getMessage());
            createSuccess = false;

        } finally {
            CarbonUtil.closeStreams(objectOutputStream, stream);
            if (createSuccess) {
                //if tmp slicemetadata creation is success, rename it to actual slicemetadata name
                CarbonFile file = FileFactory.getCarbonFile(tmpSliceMetaDataFileName, fileType);
                return file.renameForce(presentSliceMetaDataFileName);
            }
        }

        return createSuccess;
    }
}
