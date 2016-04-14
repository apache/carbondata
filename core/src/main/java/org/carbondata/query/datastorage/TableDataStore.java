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

package org.carbondata.query.datastorage;

import java.util.*;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.common.logging.impl.StandardLogService;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.columnar.impl.MultiDimKeyVarLengthEquiSplitGenerator;
import org.carbondata.core.keygenerator.columnar.impl.MultiDimKeyVarLengthVariableSplitGenerator;
import org.carbondata.core.metadata.CarbonMetadata.Cube;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.core.metadata.CarbonMetadata.Measure;
import org.carbondata.core.metadata.SliceMetaData;
import org.carbondata.core.carbon.SqlStatement;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.vo.HybridStoreModel;
import org.carbondata.query.datastorage.storeInterfaces.DataStore;
import org.carbondata.query.datastorage.storeInterfaces.DataStoreBlock;
import org.carbondata.query.datastorage.storeInterfaces.KeyValue;
import org.carbondata.query.datastorage.streams.DataInputStream;
import org.carbondata.query.datastorage.tree.CSBTree;
import org.carbondata.query.scanner.Scanner;
import org.carbondata.query.util.CarbonDataInputStreamFactory;
import org.carbondata.query.util.CarbonEngineLogEvent;

public class TableDataStore {

    /**
     * Attribute for Carbon LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(TableDataStore.class.getName());
    /**
     *
     */
    protected String factTableColumn;
    protected SqlStatement.Type[] dataTypes;
    /**
     *
     */
    private String tableName;

    //    private static final String COMMA = ", ";
    /**
     *
     */
    private KeyGenerator keyGenerator;
    /**
     * tree holds data of the fact table.
     */
    private DataStore data;
    /**
     * startKey
     */
    private byte[] startKey;
    /**
     *
     */
    private Cube metaCube;
    /**
     *
     */
    private List<String> aggregateNames;
    /**
     *
     */
    private int[] msrOrdinal;
    /**
     * meta
     */
    private SliceMetaData smd;
    /**
     * unique value
     */
    private Object[] uniqueValue;
    /**
     * min value
     */
    private Object[] minValue;
    /**
     * min value
     */
    private Object[] minValueFactForAgg;
    /**
     * type
     */
    private char[] type;
    private boolean isColumnar;
    private boolean[] aggKeyBlock;
    private int[] dimCardinality;
    private HybridStoreModel hybridStoreModel;

    public TableDataStore(String table, Cube metaCube, SliceMetaData smd, KeyGenerator keyGenerator,
            int[] dimCardinality, HybridStoreModel hybridStoreModel) {
        this.hybridStoreModel = hybridStoreModel;
        factTableColumn = metaCube.getFactCountColMapping(table);
        tableName = table;
        this.metaCube = metaCube;

        boolean hasFactCount = hasFactCount();
        this.smd = smd;
        List<Measure> measures = metaCube.getMeasures(table);
        prepareComplexDimensions(metaCube.getDimensions(table));
        aggregateNames = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
        if (hasFactCount) {
            msrOrdinal = new int[measures.size() + 1];
        } else {
            msrOrdinal = new int[measures.size()];
        }
        int len = 0;
        for (Measure measure : measures) {
            aggregateNames.add(measure.getAggName());
            msrOrdinal[len] = len;
            len++;
        }
        if (hasFactCount) {
            aggregateNames.add("sum");
            msrOrdinal[len] = len;
        }
        this.keyGenerator = keyGenerator;
        this.dimCardinality = new int[dimCardinality.length];
        System.arraycopy(dimCardinality, 0, this.dimCardinality, 0, dimCardinality.length);
    }

    /**
     * @return
     */
    public String getFactTableColumn() {
        return factTableColumn;
    }

    /**
     * @return
     */
    public boolean hasFactCount() {
        return factTableColumn != null && factTableColumn.length() > 0;
    }

    private void prepareComplexDimensions(List<Dimension> currentDimTables) {
        Map<String, ArrayList<Dimension>> complexDimensions =
                new HashMap<String, ArrayList<Dimension>>();
        for (int i = 0; i < currentDimTables.size(); i++) {
            ArrayList<Dimension> dimensions =
                    complexDimensions.get(currentDimTables.get(i).getHierName());
            if (dimensions != null) {
                dimensions.add(currentDimTables.get(i));
            } else {
                dimensions = new ArrayList<Dimension>();
                dimensions.add(currentDimTables.get(i));
            }
            complexDimensions.put(currentDimTables.get(i).getHierName(), dimensions);
        }

        for (Map.Entry<String, ArrayList<Dimension>> entry : complexDimensions.entrySet()) {
            int[] blockIndexsForEachComplexType = new int[entry.getValue().size()];
            for (int i = 0; i < entry.getValue().size(); i++) {
                blockIndexsForEachComplexType[i] = entry.getValue().get(i).getDataBlockIndex();
            }
            entry.getValue().get(0).setAllApplicableDataBlockIndexs(blockIndexsForEachComplexType);
        }
    }

    /**
     * Gets the DataStore
     *
     * @param keyGen
     * @param msrCount
     * @return
     */
    protected DataStore getDataStoreDS(KeyGenerator keyGen, int msrCount, int[] keyblockSize,
            boolean[] aggKeyBlock, boolean isColumnar) {
        boolean isFileStore = false;
        // Get the mode from cube
        // This will either be file or in-memory
        // Cube logic ensures that only these two will come here.
        String schemaAndcubeName = metaCube.getCubeName();
        String schemaName = metaCube.getSchemaName();
        String cubeName = schemaAndcubeName
                .substring(schemaAndcubeName.indexOf(schemaName + '_') + schemaName.length() + 1,
                        schemaAndcubeName.length());
        String modeValue = metaCube.getMode();
        if (modeValue.equalsIgnoreCase(CarbonCommonConstants.CARBON_MODE_DEFAULT_VAL)) {
            isFileStore = true;
        }
        boolean isForcedInMemoryCube = Boolean.parseBoolean(CarbonProperties.getInstance()
                .getProperty(CarbonCommonConstants.IS_FORCED_IN_MEMORY_CUBE,
                        CarbonCommonConstants.IS_FORCED_IN_MEMORY_CUBE_DEFAULT_VALUE));
        if (isForcedInMemoryCube) {
            isFileStore = false;
        }
        LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                "Mode set for cube " + schemaName + ':' + cubeName + "as mode=" + (isFileStore ?
                        "file" :
                        "In-Memory"));
        if (isColumnar) {
            return new CSBTree(this.hybridStoreModel, keyGen, msrCount, tableName, isFileStore,
                    keyblockSize, aggKeyBlock);
        } else {
            return new CSBTree(keyGen, msrCount, tableName, isFileStore);
        }
    }

    public boolean loadDataFromFile(String filesLocaton, int startAndEndKeySize) {
        // added for get the MDKey size by liupeng 00204190.
        CarbonFile file =
                FileFactory.getCarbonFile(filesLocaton, FileFactory.getFileType(filesLocaton));
        boolean hasFactCount = hasFactCount();
        int numberOfValues = metaCube.getMeasures(tableName).size() + (hasFactCount ? 1 : 0);
        StandardLogService
                .setThreadName(StandardLogService.getPartitionID(metaCube.getOnlyCubeName()), null);
        checkIsColumnar(numberOfValues);
        //        int keySize = keyGenerator.getKeySizeInBytes();
        int keySize = startAndEndKeySize;
        int msrCount = smd.getMeasures().length;
        List<DataInputStream> streams =
                new ArrayList<DataInputStream>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
        if (file.isDirectory()) {
            //Verify any update status fact file is present so that the original fact will be ignored since
            //updation has happened as per retention policy.
            CarbonFile[] files = getCarbonFactFilesList(file);

            if (files.length == 0) {
                LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                        "@@@@ Fact file is missing for the table :" + tableName + " @@@@");
                return false;
            }
            files = removeFactFileWithDeleteStatus(files);
            files = getCarbonFactFilesWithUpdateStatus(files);
            if (files.length == 0) {
                return false;
            }
            for (CarbonFile aFile : files) {
                streams.add(CarbonDataInputStreamFactory
                        .getDataInputStream(aFile.getAbsolutePath(), keySize, msrCount,
                                hasFactCount(), filesLocaton, tableName,
                                FileFactory.getFileType(filesLocaton)));
            }
        }

        // Initialize the stream readers
        int streamCount = streams.size();
        for (int streamCounter = 0; streamCounter < streamCount; streamCounter++) {
            streams.get(streamCounter).initInput();
        }
        //Coverity Fix add null check
        ValueCompressionModel valueCompressionMode = streams.get(0).getValueCompressionMode();
        if (null != valueCompressionMode) {
            this.uniqueValue = valueCompressionMode.getUniqueValue();
            this.minValue = valueCompressionMode.getMinValue();
            this.minValueFactForAgg = valueCompressionMode.getMinValueFactForAgg();
            this.type = valueCompressionMode.getType();
        }

        // Build tree from streams
        try {
            long t1 = System.currentTimeMillis();
            if (!isColumnar) {
                data.build(streams, hasFactCount());
            } else {
                data.buildColumnar(streams, hasFactCount(), metaCube);
            }
            //            }

            LOGGER.debug(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                    "Fact increamental load build time is: " + (System.currentTimeMillis() - t1));
        } catch (Exception e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e, e.getMessage());
        }

        // Close the readers
        for (int streamCounter = 0; streamCounter < streamCount; streamCounter++) {
            streams.get(streamCounter).closeInput();
        }

        startKey = streams.get(0).getStartKey();
        return true;
    }

    private CarbonFile[] removeFactFileWithDeleteStatus(CarbonFile[] files) {
        List<CarbonFile> listOfFactFileWithDelStatus = new ArrayList<CarbonFile>(files.length);
        Collections.addAll(listOfFactFileWithDelStatus, files);
        for (CarbonFile carbonFile : files) {
            if (carbonFile.getName().endsWith(CarbonCommonConstants.FACT_DELETE_EXTENSION)) {
                for (CarbonFile carbonArrayFiles : files) {
                    String factFileNametoRemove = carbonArrayFiles.getName().substring(0,
                            carbonFile.getName()
                                    .indexOf(CarbonCommonConstants.FACT_DELETE_EXTENSION));
                    if (carbonArrayFiles.getName().equals(factFileNametoRemove)) {
                        listOfFactFileWithDelStatus.remove(carbonArrayFiles);
                        listOfFactFileWithDelStatus.remove(carbonFile);
                    }
                }
            }
        }
        CarbonFile[] fileModified = new CarbonFile[listOfFactFileWithDelStatus.size()];
        return listOfFactFileWithDelStatus.toArray(fileModified);
    }

    private CarbonFile[] getCarbonFactFilesWithUpdateStatus(CarbonFile[] files) {
        List<CarbonFile> carbonFileList = new ArrayList<CarbonFile>(files.length);

        for (CarbonFile carbonFactFile : files) {
            if (carbonFactFile.getName().endsWith(CarbonCommonConstants.FACT_UPDATE_EXTENSION)) {
                carbonFileList.add(carbonFactFile);
            }
        }
        if (carbonFileList.size() > 0) {
            files = carbonFileList.toArray(new CarbonFile[carbonFileList.size()]);

        }
        return files;
    }

    /**
     * @param file
     * @return
     */
    private CarbonFile[] getCarbonFactFilesList(CarbonFile file) {
        CarbonFile[] files = file.listFiles(new CarbonFileFilter() {
            public boolean accept(CarbonFile pathname) {
                //verifying whether any fact file has been in update status as per retention policy.
                boolean status =
                        (!pathname.isDirectory()) && pathname.getName().startsWith(tableName)
                                && pathname.getName()
                                .endsWith(CarbonCommonConstants.FACT_UPDATE_EXTENSION);
                if (status) {
                    return true;
                }
                status = (!pathname.isDirectory()) && pathname.getName().startsWith(tableName)
                        && pathname.getName().endsWith(CarbonCommonConstants.FACT_DELETE_EXTENSION);
                if (status) {
                    return true;
                }
                return (!pathname.isDirectory()) && pathname.getName().startsWith(tableName)
                        && pathname.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT);
            }

        });

        //             Sort the fact files as per index number. (Expected names
        //             filename_1,filename_2)
        Arrays.sort(files, new Comparator<CarbonFile>() {
            public int compare(CarbonFile o1, CarbonFile o2) {
                try {
                    int f1 = Integer.parseInt(
                            o1.getName().substring(tableName.length() + 1).split("\\.")[0]);
                    int f2 = Integer.parseInt(
                            o2.getName().substring(tableName.length() + 1).split("\\.")[0]);
                    return (f1 < f2) ? -1 : (f1 == f2 ? 0 : 1);
                } catch (Exception e) {
                    LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e.getMessage());
                    return o1.getName().compareTo(o2.getName());
                }
            }
        });
        return files;
    }

    /**
     * @param numberOfValues
     */
    private void checkIsColumnar(int numberOfValues) {
        isColumnar = Boolean.parseBoolean(CarbonCommonConstants.IS_COLUMNAR_STORAGE_DEFAULTVALUE);

        if (isColumnar) {
            int dimSet = Integer.parseInt(
                    CarbonCommonConstants.DIMENSION_SPLIT_VALUE_IN_COLUMNAR_DEFAULTVALUE);

            if (!isColumnar) {
                dimSet = keyGenerator.getDimCount();
            }
            int[] keyBlockSize = null;

            // if there is no single dims present (i.e only high card dims is present.)
            if (this.dimCardinality.length > 0) {
                keyBlockSize = new MultiDimKeyVarLengthVariableSplitGenerator(CarbonUtil
                        .getDimensionBitLength(this.hybridStoreModel.getHybridCardinality(),
                                this.hybridStoreModel.getDimensionPartitioner()),
                        this.hybridStoreModel.getColumnSplit()).getBlockKeySize();

                boolean isAggKeyBlock = Boolean.parseBoolean(
                        CarbonCommonConstants.AGGREAGATE_COLUMNAR_KEY_BLOCK_DEFAULTVALUE);
                if (isAggKeyBlock) {
                    int noDictionaryValue = Integer.parseInt(CarbonProperties.getInstance()
                            .getProperty(CarbonCommonConstants.HIGH_CARDINALITY_VALUE,
                                    CarbonCommonConstants.HIGH_CARDINALITY_VALUE_DEFAULTVALUE));
                    int aggIndex = 0;
                    if (this.hybridStoreModel.isHybridStore()) {
                        this.aggKeyBlock =
                                new boolean[this.hybridStoreModel.getColumnStoreOrdinals().length
                                        + 1];
                        this.aggKeyBlock[aggIndex++] = false;
                    } else {
                        this.aggKeyBlock =
                                new boolean[this.hybridStoreModel.getColumnStoreOrdinals().length];
                    }

                    for (int i = hybridStoreModel.getRowStoreOrdinals().length;
                         i < dimCardinality.length; i++) {
                        if (dimCardinality[i] == 0) {
                            continue;
                        }
                        if (dimCardinality[i] < noDictionaryValue) {
                            this.aggKeyBlock[aggIndex++] = true;
                            continue;
                        }
                        aggIndex++;
                    }
                }

            } else {
                keyBlockSize = new int[0];
                aggKeyBlock = new boolean[0];
            }
            data = getDataStoreDS(keyGenerator, numberOfValues, keyBlockSize, aggKeyBlock, true);
        } else {

            data = getDataStoreDS(keyGenerator, numberOfValues, null, null, false);
        }
    }

    private int[] getKeyBlockSizeWithComplexTypes(int[] dimCardinality) {
        int[] keyBlockSize = new int[dimCardinality.length];
        for (int i = 0; i < dimCardinality.length; i++) {
            if (dimCardinality[i] == 0) keyBlockSize[i] = 8;
            else keyBlockSize[i] =
                    new MultiDimKeyVarLengthEquiSplitGenerator(new int[] { dimCardinality[i] },
                            (byte) 1).getBlockKeySize()[0];
        }
        return keyBlockSize;
    }

    public KeyValue getData(byte[] key, Scanner scanner) {
        return data.get(key, scanner);
    }

    public void initializeScanner(byte[] key, Scanner scanner) {
        data.getNext(key, scanner);
    }

    public KeyValue getNextAvailableData(byte[] key, Scanner scanner) {
        return data.getNext(key, scanner);
    }

    public DataStoreBlock getDataStoreBlock(byte[] key, FileHolder fileHolder, boolean isFirst) {
        return data.getBlock(key, fileHolder, isFirst);
    }

    public long getSize() {
        return data.size();
    }

    public void clear() {
        data = null;
    }

    public long[][] getDataStoreRange() {
        return data.getRanges();
    }

    /**
     * @return the data
     */
    public DataStore getData() {
        return data;
    }

    public int[] getMsrOrdinal() {
        return msrOrdinal;
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public KeyGenerator getKeyGenerator() {
        return keyGenerator;
    }

    public Object[] getUniqueValue() {
        return uniqueValue;
    }

    public SqlStatement.Type[] getDataTypes() {
        return dataTypes;
    }

    public void setDataTypes(SqlStatement.Type[] dataTypes) {
        this.dataTypes = dataTypes;
    }

    public Object[] getMinValue() {
        return minValue;
    }

    /**
     * @return the type
     */
    public char[] getType() {
        return type;
    }

    /**
     * @return the minValueFactForAgg
     */
    public Object[] getMinValueFactForAgg() {
        return minValueFactForAgg;
    }

    /**
     * @return the aggKeyBlock
     */
    public boolean[] getAggKeyBlock() {
        return aggKeyBlock;
    }

    /**
     * @param aggKeyBlock the aggKeyBlock to set
     */
    public void setAggKeyBlock(boolean[] aggKeyBlock) {
        this.aggKeyBlock = aggKeyBlock;
    }

    public int[] getDimCardinality() {
        return dimCardinality;
    }

    public void setDimCardinality(int[] dimCardinality) {
        this.dimCardinality = dimCardinality;
    }
}
