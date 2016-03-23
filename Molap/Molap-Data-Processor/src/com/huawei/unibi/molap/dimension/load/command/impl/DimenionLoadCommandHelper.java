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

package com.huawei.unibi.molap.dimension.load.command.impl;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.dimension.load.info.DimensionLoadInfo;
import com.huawei.unibi.molap.file.manager.composite.FileData;
import com.huawei.unibi.molap.file.manager.composite.IFileManagerComposite;
import com.huawei.unibi.molap.file.manager.composite.LoadFolderData;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.merger.Util.MolapSliceMergerUtil;
import com.huawei.unibi.molap.metadata.SliceMetaData;
import com.huawei.unibi.molap.surrogatekeysgenerator.csvbased.MolapCSVBasedDimSurrogateKeyGen;
import com.huawei.unibi.molap.surrogatekeysgenerator.csvbased.RealTimeDataPropertyReader;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;
import com.huawei.unibi.molap.util.MolapProperties;
import com.huawei.unibi.molap.util.MolapUtil;
import com.huawei.unibi.molap.util.MolapUtilException;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.pentaho.di.core.exception.KettleException;

public final class DimenionLoadCommandHelper {
    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(DimenionLoadCommandHelper.class.getName());

    private static DimenionLoadCommandHelper instanse;
    /**
     * dataPropertyReader
     */
    protected RealTimeDataPropertyReader dataPropertyReader;

    private DimenionLoadCommandHelper() {
        //Do Nothing
    }

    public static DimenionLoadCommandHelper getInstance() {
        if (null == instanse) {
            synchronized (DimenionLoadCommandHelper.class) {
                if (null == instanse) {
                    instanse = new DimenionLoadCommandHelper();
                }
            }
        }
        return instanse;
    }

    /**
     * Recursively split the records and return the data as comes
     * under quotes.
     *
     * @param data
     * @param records For Example :
     *                If data comes like:
     *                String s = ""13569,69600000","SN=66167523766568","NE=66167522854161""
     *                then it will return {"13569,69600000","SN=66167523766568","NE=66167522854161"}
     *                and If String is without quotes:
     *                String s = "13569,69600000,SN=66167523766568,NE=66167522854161"
     *                then it will return {"13569","69600000","SN=66167523766568","NE=66167522854161"}
     */
    private static void addRecordsWithQuotesRecursively(String data, List<String> records) {
        if (data.length() == 0) {
            return;
        }

        if (data.charAt(0) != ',') {
            int secondIndexOfQuotes = data.indexOf("\"", 1);
            records.add(data.substring(1, secondIndexOfQuotes));

            if (data.length() - 1 != secondIndexOfQuotes) {
                data = data.substring(secondIndexOfQuotes + 2);
            } else {
                data = data.substring(secondIndexOfQuotes + 1);
            }
        } else {
            int secondIndexOfQuotes = data.indexOf(",");
            records.add(data.substring(0, secondIndexOfQuotes));

            if (data.length() - 1 != secondIndexOfQuotes) {
                data = data.substring(secondIndexOfQuotes + 1);
            } else {
                data = data.substring(secondIndexOfQuotes + 1);
                if (data.isEmpty()) {
                    records.add(data);
                }
            }
        }

        //call recursively
        addRecordsWithQuotesRecursively(data, records);
    }

    private static void addRecordsWithoutQuotesRecursively(String data, List<String> records) {
        if (data.length() == 0) {
            return;
        }

        int secondIndexOfQuotes = data.indexOf(",");
        records.add(data.substring(0, secondIndexOfQuotes));

        if (data.length() - 1 != secondIndexOfQuotes) {
            data = data.substring(secondIndexOfQuotes + 1);
        } else {
            data = data.substring(secondIndexOfQuotes + 1);
            if (data.isEmpty()) {
                records.add(data);
            }
        }

        //call recursively
        addRecordsWithoutQuotesRecursively(data, records);
    }

    public static void mergeFiles(String baseStorelocation, Map<String, Integer> hierKeyMap)
            throws IOException {
        // merge level Files.
        mergeLevelFiles(baseStorelocation);

        // Merge hierarchy Files.

        mergeHierarchyFiles(baseStorelocation, hierKeyMap);
    }

    public static void mergeHierarchyFiles(String baseStorelocation,
            Map<String, Integer> hierKeyMap) throws IOException {
        File loadFolder = new File(baseStorelocation);

        File[] allHierarchyFiles = loadFolder.listFiles(new FileFilter() {

            @Override public boolean accept(File pathname) {
                if (pathname.getName().indexOf(MolapCommonConstants.HIERARCHY_FILE_EXTENSION)
                        > -1) {
                    return true;
                }
                return false;
            }
        });

        File[] uniqueHierarchyFiles = loadFolder.listFiles(new FileFilter() {

            @Override public boolean accept(File file) {
                if (file.getName().indexOf(MolapCommonConstants.HIERARCHY_FILE_EXTENSION + '0')
                        > -1) {
                    return true;
                }
                return false;
            }
        });

        Map<String, List<File>> filesMap =
                new HashMap<String, List<File>>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

        if (null == uniqueHierarchyFiles) {
            return;
        }

        for (File uniqueFile : uniqueHierarchyFiles) {
            List<File> files = new ArrayList<File>(MolapCommonConstants.CONSTANT_SIZE_TEN);
            String uniqueFilename = uniqueFile.getName();

            String uniqueSubstring = uniqueFilename.substring(0, uniqueFilename.length() - 1);

            for (File levelFile : allHierarchyFiles) {
                if (levelFile.getName().startsWith(uniqueSubstring)) {
                    files.add(levelFile);
                }
            }

            filesMap.put(uniqueSubstring, files);
        }

        Set<Entry<String, List<File>>> entrySet = filesMap.entrySet();

        IFileManagerComposite fileManager = new LoadFolderData();
        fileManager.setName(baseStorelocation);

        for (Entry<String, List<File>> entry : entrySet) {
            List<File> fileToMerge = entry.getValue();

            String sourceFileName = entry.getKey() + MolapCommonConstants.FILE_INPROGRESS_STATUS;
            String destFile = baseStorelocation + File.separator + sourceFileName;
            FileData fileData = new FileData(sourceFileName, baseStorelocation);
            fileManager.add(fileData);

            MolapSliceMergerUtil.mergeHierarchyFiles(fileToMerge, new File(destFile),
                    hierKeyMap.get(entry.getKey()));

            deleteFiles(fileToMerge);

        }

        // Rename inprogess file to normal file extension.

        renameInprogressToNormalFileExtension(fileManager);

    }

    public static void mergeLevelFiles(String baseStorelocation) throws IOException {
        File loadFolder = new File(baseStorelocation);

        File[] allLevelFiles = loadFolder.listFiles(new FileFilter() {

            @Override public boolean accept(File pathname) {
                if (pathname.getName().indexOf(MolapCommonConstants.LEVEL_FILE_EXTENSION) > -1) {
                    return true;
                }
                return false;
            }
        });

        File[] uniqueLevelFiles = loadFolder.listFiles(new FileFilter() {

            @Override public boolean accept(File file) {
                if (file.getName().endsWith(MolapCommonConstants.LEVEL_FILE_EXTENSION + '0')) {
                    return true;
                }
                return false;
            }
        });

        Map<String, List<File>> filesMap =
                new HashMap<String, List<File>>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

        if (null == uniqueLevelFiles) {
            return;
        }

        for (File uniqueFile : uniqueLevelFiles) {
            List<File> files = new ArrayList<File>(MolapCommonConstants.CONSTANT_SIZE_TEN);
            String uniqueFilename = uniqueFile.getName();

            String uniqueSubString = uniqueFilename.substring(0, uniqueFilename.length() - 1);

            for (File levelFile : allLevelFiles) {
                if (levelFile.getName().startsWith(uniqueSubString)) {
                    files.add(levelFile);
                }
            }

            filesMap.put(uniqueSubString, files);
        }

        Set<Entry<String, List<File>>> entrySet = filesMap.entrySet();

        IFileManagerComposite fileMgrObj = new LoadFolderData();
        fileMgrObj.setName(baseStorelocation);

        for (Entry<String, List<File>> entry : entrySet) {
            List<File> fileToMerge = entry.getValue();

            String sourceFileName = entry.getKey() + MolapCommonConstants.FILE_INPROGRESS_STATUS;
            String destFile = baseStorelocation + File.separator + sourceFileName;
            FileData fileData = new FileData(sourceFileName, baseStorelocation);
            fileMgrObj.add(fileData);

            MolapSliceMergerUtil.copyMultipleFileToSingleFile(fileToMerge, new File(destFile));

            deleteFiles(fileToMerge);

        }

        // Rename inprogess file to normal file extension.

        renameInprogressToNormalFileExtension(fileMgrObj);
    }

    /**
     * @param fileToMerge
     * @throws IOException
     */
    private static void deleteFiles(List<File> fileToMerge) throws IOException {
        // after copying delete the source files.
        try {
            MolapUtil.deleteFiles(fileToMerge.toArray(new File[fileToMerge.size()]));
        } catch (MolapUtilException e) {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Not able to delete the files" + fileToMerge.toString());
            throw new IOException(e);
        }
    }

    /**
     * @throws IOException
     */
    private static void renameInprogressToNormalFileExtension(IFileManagerComposite fileManager)
            throws IOException {

        if (null == fileManager || fileManager.size() == 0) {
            return;
        }
        int fileMangerSize = fileManager.size();

        for (int i = 0; i < fileMangerSize; i++) {
            FileData memberFile = (FileData) fileManager.get(i);
            String msrLvlInProgressFileName = memberFile.getFileName();

            String storePath = memberFile.getStorePath();
            String changedFileName = msrLvlInProgressFileName
                    .substring(0, msrLvlInProgressFileName.lastIndexOf('.'));
            File currentFile = new File(storePath + File.separator + msrLvlInProgressFileName);
            File destFile = new File(storePath + File.separator + changedFileName);

            if (!currentFile.renameTo(destFile)) {
                LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Not able to rename the level Files to normal format");
                throw new IOException("Not able to rename the level Files to normal format");
            }
        }

    }

    public static Map<String, Integer> getHierKeyMap(Map<String, KeyGenerator> keyGenerator) {
        Map<String, Integer> hierKeyMap =
                new HashMap<String, Integer>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

        Set<Entry<String, KeyGenerator>> entrySet = keyGenerator.entrySet();

        for (Entry<String, KeyGenerator> entry : entrySet) {
            KeyGenerator keyGen = entry.getValue();
            hierKeyMap.put(entry.getKey() + MolapCommonConstants.HIERARCHY_FILE_EXTENSION,
                    keyGen.getKeySizeInBytes());
        }

        return hierKeyMap;
    }

    /**
     * Check the cache exist , if exists return true , false otherwise.
     *
     * @param tableName
     * @param columnPropMap
     * @param dimensionLoadInfo
     * @return
     */
    public boolean isDimCacheExist(String[] actualColumns, String tableName,
            Map<String, String[]> columnPropMap, DimensionLoadInfo dimensionLoadInfo) {
        MolapCSVBasedDimSurrogateKeyGen surrogateKeyGen = dimensionLoadInfo.getSurrogateKeyGen();
        Map<String, Map<String, Integer>> memberCache = surrogateKeyGen.getMemberCache();

        if (null == actualColumns || !(actualColumns.length > 0)) {
            return true;
        }

        int columnCount = actualColumns.length;
        int actualColCount = 0;

        Map<String, Boolean> dimColumnProcessed =
                new HashMap<String, Boolean>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

        for (String columnName : actualColumns) {
            // For property column need to add check , as for property column files will not be created
            // and cache will not be present.
            String dimColumnName = tableName + '_' + columnName.trim();
            if (columnPropMap.containsKey(dimColumnName)) {
                if (null == dimColumnProcessed.get(dimColumnName)) {
                    actualColCount += columnPropMap.get(dimColumnName).length;
                    dimColumnProcessed.put(dimColumnName, true);
                } else {
                    actualColCount++;
                    continue;
                }
            }
            Map<String, Integer> memberMap = memberCache.get(dimColumnName);
            if (null != memberMap && memberMap.size() > 0) {
                actualColCount++;
            }
        }

        if (actualColCount < columnCount) {
            return false;
        }

        return true;

    }

    /**
     * This method will check whether the hier cache exists or not for the
     * hierarchy we are going to load, if loaded then return true, false otherwise.
     *
     * @param hierarichiesName
     * @return
     */
    public boolean isHierCacheExist(String hierarichiesName, DimensionLoadInfo dimensionLoadInfo) {
        MolapCSVBasedDimSurrogateKeyGen surrogateKeyGen = dimensionLoadInfo.getSurrogateKeyGen();
        Map<String, Int2ObjectMap<int[]>> hierCache = surrogateKeyGen.getHierCache();

        Int2ObjectMap<int[]> int2ObjectMap = hierCache.get(hierarichiesName);
        if (null != int2ObjectMap && int2ObjectMap.size() > 1) {
            return true;
        }

        return false;
    }

    /**
     * @param tableName
     * @return
     * @throws KettleException
     */
    public boolean checkModifiedTableInSliceMetaData(String tableName,
            DimensionLoadInfo dimensionLoadInfo, int currentRestructNumber) throws KettleException {
        String storeLocation =
                updateStoreLocationAndPopulateMolapInfo(dimensionLoadInfo.getMeta().getSchemaName(),
                        dimensionLoadInfo.getMeta().getCubeName());
        //

        int restructFolderNumber = currentRestructNumber;

        String sliceMetaDataFilePath =
                storeLocation + File.separator + MolapCommonConstants.RESTRUCTRE_FOLDER
                        + restructFolderNumber + File.separator + dimensionLoadInfo.getMeta()
                        .getTableName() + File.separator + MolapUtil
                        .getSliceMetaDataFileName(currentRestructNumber);

        if (!(new File(sliceMetaDataFilePath).exists())) {
            return true;
        } else {
            SliceMetaData sliceMetaData = null;
            FileInputStream fileInputStream = null;
            ObjectInputStream objectInputStream = null;
            //
            try {
                fileInputStream = new FileInputStream(sliceMetaDataFilePath);
                objectInputStream = new ObjectInputStream(fileInputStream);
                sliceMetaData = (SliceMetaData) objectInputStream.readObject();
                //
            } catch (FileNotFoundException e) {
                throw new KettleException("slice metadata file not found", e);
            } catch (IOException e) {
                throw new KettleException("Not able to read slice metadata File", e);
            } catch (ClassNotFoundException e) {
                throw new KettleException("SliceMetaData class not found.", e);
            } finally {
                MolapUtil.closeStreams(fileInputStream, objectInputStream);
            }

            if (null == sliceMetaData.getTableNamesToLoadMandatory()) {
                return true;
            } else {
                Set<String> tableNamesToLoadMandatory =
                        sliceMetaData.getTableNamesToLoadMandatory();
                if (tableNamesToLoadMandatory.contains(tableName)) {
                    return false;
                }
            }
        }

        return true;
    }

    private String updateStoreLocationAndPopulateMolapInfo(String schemaName, String cubeName) {
        String tempLocationKey = schemaName + '_' + cubeName;
        String schemaCubeName = schemaName + '/' + cubeName;
        String storeLocation = MolapProperties.getInstance()
                .getProperty(tempLocationKey, MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL);
        File f = new File(storeLocation);
        String absoluteStrPath = f.getAbsolutePath();
        //
        if (absoluteStrPath.length() > 0
                && absoluteStrPath.charAt(absoluteStrPath.length() - 1) == '/') {
            absoluteStrPath = absoluteStrPath + schemaCubeName;
        } else {
            absoluteStrPath =
                    absoluteStrPath + System.getProperty("file.separator") + schemaCubeName;
        }
        return absoluteStrPath;
    }

    /**
     * This method will take the dimension tableNames String and
     * return the string array
     *
     * @param dimTableNames
     * @return
     */
    public String[] getDimensionTableNameArray(String[] dimTableNames) {
        for (int i = 0; i < dimTableNames.length; i++) {
            if (dimTableNames[i].indexOf("\"") > -1) {
                dimTableNames[i] = dimTableNames[i].replace("\"", "").trim();
            }
        }

        return dimTableNames;
    }

    /**
     * Below method will be used to member cache
     *
     * @param levelTypeColumnMap
     * @param tableName
     * @param columnIndex
     * @param columnNames
     * @param columnAndMemberListMap
     */
    public void updateMemberCache(DimensionLoadInfo dimensionLoadInfo,
            Map<String, String> levelTypeColumnMap, String tableName, String[] columnNames,
            int[] columnIndex, Map<String, Set<String>> columnAndMemberListMap,
            Map<String, Integer> levelAndCardinalityMap) {
        MolapCSVBasedDimSurrogateKeyGen surrogateKeyGen = dimensionLoadInfo.getSurrogateKeyGen();

        surrogateKeyGen.setTimDimMax(new int[surrogateKeyGen.getDimsFiles().length]);
        surrogateKeyGen.setTimeDimCache(new HashMap<String, Map<String, Integer>>(
                MolapCommonConstants.DEFAULT_COLLECTION_SIZE));

        if (null == levelTypeColumnMap || levelTypeColumnMap.isEmpty()) {
            return;
        }

        dataPropertyReader = new RealTimeDataPropertyReader(
                dimensionLoadInfo.getMeta().getSchemaName() + '/' + dimensionLoadInfo.getMeta()
                        .getCubeName(), columnAndMemberListMap, levelTypeColumnMap,
                levelAndCardinalityMap);

        Map<String, Map<String, Integer>> memberCache = surrogateKeyGen.getMemberCache();

        String yearColumn = levelTypeColumnMap.get("YEAR");
        if (null != yearColumn) {
            Map<String, Integer> yearMap = memberCache.get(tableName + '_' + yearColumn);

            surrogateKeyGen.getTimeDimCache()
                    .put(tableName + '_' + yearColumn, new HashMap<String, Integer>(yearMap));

            yearMap.putAll(dataPropertyReader.getYearMap());

        }
        String monthColumn = levelTypeColumnMap.get("MONTHS");
        if (null != monthColumn) {
            Map<String, Integer> monthsMap = memberCache.get(tableName + '_' + monthColumn);

            surrogateKeyGen.getTimeDimCache()
                    .put(tableName + '_' + monthColumn, new HashMap<String, Integer>(monthsMap));

            monthsMap.putAll(dataPropertyReader.getMonthMap());

        }

        String dayColumn = levelTypeColumnMap.get("DAYS");
        if (null != dayColumn) {
            Map<String, Integer> dayMap = memberCache.get(tableName + '_' + dayColumn);

            surrogateKeyGen.getTimeDimCache()
                    .put(tableName + '_' + dayColumn, new HashMap<String, Integer>(dayMap));

            dayMap.putAll(dataPropertyReader.getDayMap());

        }

    }

    /**
     * @param originalColumnNames
     */
    public String[] checkQuotesAndAddTableName(String[] originalColumnNames, String tableName,
            String[] trimmedoriginalColName) {
        //
        String[] result = new String[originalColumnNames.length];
        int i = 0;
        //
        for (int j = 0; j < originalColumnNames.length; j++) {
            String str = originalColumnNames[j];
            result[i] = tableName + '_' + str.trim();
            trimmedoriginalColName[i] = str.trim();
            i++;
        }
        //
        return result;

    }

    /**
     * @param originalColumnNames
     */
    public String[] checkQuotesAndAddTableNameForCSV(String[] originalColumnNames,
            String tableName) {
        //
        String[] columnNameWithoutprimarykey = new String[originalColumnNames.length];
        System.arraycopy(originalColumnNames, 0, columnNameWithoutprimarykey, 0,
                originalColumnNames.length);
        String[] result = new String[columnNameWithoutprimarykey.length];
        int i = 0;
        //
        for (int j = 0; j < columnNameWithoutprimarykey.length; j++) {
            String str = columnNameWithoutprimarykey[j];
            result[i] = tableName + '_' + str.trim();
            i++;
        }
        //
        return result;

    }

    /**
     * Return the index of the column
     *
     * @param columnNamesFromFile
     * @param names
     * @return
     */
    public int[] getIndex(String[] columnNamesFromFile, String[] names) {
        int[] columnIndex = new int[names.length];
        for (int i = 0; i < names.length; i++) {
            for (int j = 0; j < columnNamesFromFile.length; j++) {
                if (names[i].equalsIgnoreCase(columnNamesFromFile[j].trim())) {
                    columnIndex[i] = j;
                    break;
                }
            }
        }
        return columnIndex;
    }

    public int getRepeatedPrimaryFromLevels(String tableName, String[] columnNames,
            String primaryKey) {
        primaryKey = tableName + '_' + primaryKey.trim();
        for (int j = 0; j < columnNames.length; j++) {
            if (primaryKey.equals(columnNames[j])) {
                return j;
            }
        }
        return -1;
    }

    /**
     * Return the data row
     *
     * @param data
     * @return
     */
    public String[] getRowData(String data) {
        if (data.indexOf("\"") != 0 && data.lastIndexOf("\"") != data.length()) {
            if (data.lastIndexOf(",") == data.length() - 1) {
                return getData(data);
            } else {
                return data.split(",");
            }
        }

        List<String> records = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);

        addRecordsWithQuotesRecursively(data, records);

        return records.toArray(new String[records.size()]);
    }

    private String[] getData(String data) {
        List<String> records = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);

        addRecordsWithoutQuotesRecursively(data, records);

        return records.toArray(new String[records.size()]);
    }

}

