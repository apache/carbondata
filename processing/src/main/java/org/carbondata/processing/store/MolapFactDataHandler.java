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

package org.carbondata.processing.store;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.datastorage.store.NodeKeyStore;
import org.carbondata.core.datastorage.store.NodeMeasureDataStore;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.datastorage.store.dataholder.MolapWriteDataHolder;
import org.carbondata.core.datastorage.util.StoreFactory;
import org.carbondata.query.cache.QueryExecutorUtil;
import org.carbondata.core.file.manager.composite.FileData;
import org.carbondata.core.file.manager.composite.IFileManagerComposite;
import org.carbondata.core.file.manager.composite.LoadFolderData;
import org.carbondata.processing.groupby.MolapAutoAggGroupBy;
import org.carbondata.processing.groupby.MolapAutoAggGroupByExtended;
import org.carbondata.processing.groupby.exception.MolapGroupByException;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.carbondata.processing.store.writer.MolapDataWriter;
import org.carbondata.processing.store.writer.exception.MolapDataWriterException;
import org.carbondata.processing.util.MolapDataProcessorLogEvent;
import org.carbondata.processing.util.MolapDataProcessorUtil;
import org.carbondata.core.util.MolapProperties;
import org.carbondata.core.util.ValueCompressionUtil;

public class MolapFactDataHandler implements MolapFactHandler {

    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(MolapFactDataHandler.class.getName());

    /**
     * data writer
     */
    private MolapDataWriter dataWriter;

    /**
     * File manager
     */
    private IFileManagerComposite fileManager;

    /**
     * total number of entries in leaf node
     */
    private int entryCount;

    /**
     * startkey of each node
     */
    private byte[] startKey;

    /**
     * end key of each node
     */
    private byte[] endKey;

    /**
     * ValueCompressionModel
     */
    private ValueCompressionModel compressionModel;

    /**
     * createKeyStore object which will hold the mdkey
     */
    private NodeKeyStore keyStore;

    /**
     * data store which will hold the measure data
     */
    private NodeMeasureDataStore dataStore;

    /**
     * measure count
     */
    private int measureCount;

    /**
     * index of mdkey in incoming rows
     */
    private int mdKeyIndex;

    /**
     * uniqueValue
     */
    private Object[] uniqueValue;

    /**
     * leaf node size
     */
    private int leafNodeSize;

    /**
     * isGroupByEnabled
     */
    private boolean isGroupByEnabled;

    /**
     * groupBy
     */
    private MolapAutoAggGroupBy groupBy;

    /**
     * mdkeyLength
     */
    private int mdkeyLength;

    /**
     * storeLocation
     */
    private String storeLocation;

    /**
     * tableName
     */
    private String tableName;

    /**
     * schemaName
     */
    private String schemaName;

    /**
     * cubeName
     */
    private String cubeName;

    /**
     * aggregators
     */
    private String[] aggregators;

    /**
     * aggregatorClass
     */
    private String[] aggregatorClass;

    /**
     * MolapWriteDataHolder
     */
    private MolapWriteDataHolder[] dataHolder;

    /**
     * factDimLens
     */
    private int[] factDimLens;

    /**
     * isMergingRequest
     */
    private boolean isMergingRequestForCustomAgg;

    /**
     * otherMeasureIndex
     */
    private int[] otherMeasureIndex;

    /**
     * customMeasureIndex
     */
    private int[] customMeasureIndex;

    /**
     * isUpdateMemberRequest
     */
    private boolean isUpdateMemberRequest;

    /**
     * factLevels
     */
    private int[] surrogateIndex;

    /**
     * factKeyGenerator
     */
    private KeyGenerator factKeyGenerator;

    /**
     * aggKeyGenerator
     */
    private KeyGenerator keyGenerator;

    /**
     * maskedByteRanges
     */
    private int[] maskedByte;

    /**
     * isDataWritingRequest
     */
    private boolean isDataWritingRequest;

    private int currentRestructNumber;

    /**
     * MolapFactDataHandler cosntructor
     *
     * @param schemaName
     * @param cubeName
     * @param tableName
     * @param isGroupByEnabled
     * @param measureCount
     * @param mdkeyLength
     * @param mdKeyIndex
     * @param aggregators
     * @param aggregatorClass
     */
    public MolapFactDataHandler(String schemaName, String cubeName, String tableName,
            boolean isGroupByEnabled, int measureCount, int mdkeyLength, int mdKeyIndex,
            String[] aggregators, String[] aggregatorClass, String storeLocation, int[] factDimLens,
            boolean isMergingRequestForCustomAgg, boolean isUpdateMemberRequest, int[] dimLens,
            String[] factLevels, String[] aggLevels, boolean isDataWritingRequest,
            int currentRestructNum) {
        this.schemaName = schemaName;
        this.cubeName = cubeName;
        this.tableName = tableName;
        this.isGroupByEnabled = isGroupByEnabled;
        this.measureCount = measureCount;
        this.mdkeyLength = mdkeyLength;
        this.mdKeyIndex = mdKeyIndex;
        this.aggregators = aggregators;
        this.aggregatorClass = aggregatorClass;
        this.storeLocation = storeLocation;
        this.factDimLens = factDimLens;
        this.isMergingRequestForCustomAgg = isMergingRequestForCustomAgg;
        this.isUpdateMemberRequest = isUpdateMemberRequest;
        this.isDataWritingRequest = isDataWritingRequest;
        this.currentRestructNumber = currentRestructNum;
        if (this.isGroupByEnabled && isDataWritingRequest && !isUpdateMemberRequest) {
            surrogateIndex = new int[aggLevels.length];
            Arrays.fill(surrogateIndex, -1);
            for (int i = 0; i < aggLevels.length; i++) {
                for (int j = 0; j < factLevels.length; j++) {
                    if (aggLevels[i].equals(factLevels[j])) {
                        surrogateIndex[i] = j;
                        break;
                    }
                }
            }
            this.factKeyGenerator = KeyGeneratorFactory.getKeyGenerator(factDimLens);
            this.keyGenerator = KeyGeneratorFactory.getKeyGenerator(dimLens);
            int[] maskedByteRanges =
                    MolapDataProcessorUtil.getMaskedByte(surrogateIndex, factKeyGenerator);
            this.maskedByte = new int[factKeyGenerator.getKeySizeInBytes()];
            QueryExecutorUtil.updateMaskedKeyRanges(maskedByte, maskedByteRanges);
        }
    }

    /**
     * This method will be used to get and update the step properties which will
     * required to run this step
     *
     * @throws MolapDataWriterException
     */
    public void initialise() throws MolapDataWriterException {
        fileManager = new LoadFolderData();
        fileManager.setName(new File(this.storeLocation).getName());
        if (!isGroupByEnabled || this.isUpdateMemberRequest) {
            try {
                setWritingConfiguration(this.mdkeyLength);
            } catch (MolapDataWriterException e) {
                throw e;
            }
        } else {
            if (!isMergingRequestForCustomAgg) {
                this.groupBy =
                        new MolapAutoAggGroupBy(aggregators, aggregatorClass, this.schemaName,
                                this.cubeName, this.tableName, this.factDimLens,
                                MolapCommonConstants.FILE_INPROGRESS_STATUS, currentRestructNumber);
            } else {
                this.groupBy = new MolapAutoAggGroupByExtended(aggregators, aggregatorClass,
                        this.schemaName, this.cubeName, this.tableName, this.factDimLens,
                        MolapCommonConstants.FILE_INPROGRESS_STATUS, currentRestructNumber);
            }
        }

    }

    /**
     * This method will add mdkey and measure values to store
     *
     * @param rowObj
     * @throws MolapDataWriterException
     */
    public void addDataToStore(Object[] rowObj) throws MolapDataWriterException {
        if (isGroupByEnabled && !this.isUpdateMemberRequest) {
            try {
                groupBy.add(rowObj);
            } catch (MolapGroupByException e) {
                throw new MolapDataWriterException("Problem in doing groupBy", e);
            }
        } else {
            addToStore(rowObj);
        }
    }

    /**
     * below method will be used to add row to store
     *
     * @param row
     * @throws MolapDataWriterException
     */
    private void addToStore(Object[] row) throws MolapDataWriterException {
        byte[] mdkey = (byte[]) row[this.mdKeyIndex];
        ByteBuffer buffer = null;
        byte[] b = null;
        if (this.entryCount == 0) {
            this.startKey = mdkey;
        }
        this.endKey = mdkey;
        // add to key store
        this.keyStore.put(entryCount, mdkey);
        for (int i = 0; i < otherMeasureIndex.length; i++) {
            if (null == row[otherMeasureIndex[i]]) {
                dataHolder[otherMeasureIndex[i]].setWritableDoubleValueByIndex(entryCount,
                        uniqueValue[otherMeasureIndex[i]]);
            } else {
                dataHolder[otherMeasureIndex[i]]
                        .setWritableDoubleValueByIndex(entryCount, row[otherMeasureIndex[i]]);
            }
        }
        for (int i = 0; i < customMeasureIndex.length; i++) {
            b = (byte[]) row[customMeasureIndex[i]];
            if (isUpdateMemberRequest) {
                buffer = ByteBuffer.allocate(b.length + MolapCommonConstants.INT_SIZE_IN_BYTE);
                buffer.putInt(b.length);
                buffer.put(b);
                buffer.flip();
                b = buffer.array();
            }
            dataHolder[customMeasureIndex[i]].setWritableByteArrayValueByIndex(entryCount, b);
        }
        this.entryCount++;
        // if entry count reaches to leaf node size then we are ready to
        // write
        // this to leaf node file and update the intermediate files
        if (this.entryCount == this.leafNodeSize) {
            // write data to file
            this.dataWriter.writeDataToFile(this.keyStore.getWritableKeyArray(),
                    this.dataStore.getWritableMeasureDataArray(dataHolder), this.entryCount,
                    this.startKey, this.endKey);
            // set the entry count to zero
            this.entryCount = 0;
            this.keyStore.clear();
            initialisedataHolder();
        }
    }

    /**
     * below method will be used to finish the data handler
     *
     * @throws MolapDataWriterException
     */
    public void finish() throws MolapDataWriterException {
        if (isGroupByEnabled && !this.isUpdateMemberRequest) {
            try {
                this.groupBy.initiateReading(this.storeLocation, this.tableName);
                setWritingConfiguration(this.keyGenerator.getKeySizeInBytes());
                //CHECKSTYLE:OFF    Approval No:Approval-V3R8C00_018
                Object[] row = null;
                while (this.groupBy.hasNext()) { //CHECKSTYLE:ON
                    row = this.groupBy.next();
                    if (isDataWritingRequest) {
                        row[mdKeyIndex] = getAggregateTableMdkey((byte[]) row[mdKeyIndex]);
                    }
                    addToStore(row);
                }
            } catch (MolapGroupByException ex) {
                throw new MolapDataWriterException("Problem while doing the groupby", ex);
            } finally {
                try {
                    this.groupBy.finish();
                } catch (MolapGroupByException e) {
                    LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                            "Problem in group by finish");
                }
            }
        }
        // / still some data is present in stores if entryCount is more
        // than 0
        if (this.entryCount > 0) {
            // write data to file
            this.dataWriter.writeDataToFile(this.keyStore.getWritableKeyArray(),
                    this.dataStore.getWritableMeasureDataArray(dataHolder), this.entryCount,
                    this.startKey, this.endKey);
            this.dataWriter.writeleafMetaDataToFile();
        } else if (null != this.dataWriter && this.dataWriter.getMetaListSize() > 0) {
            this.dataWriter.writeleafMetaDataToFile();
        }
    }

    private byte[] getAggregateTableMdkey(byte[] maksedKey) throws MolapDataWriterException {
        long[] keyArray = this.factKeyGenerator.getKeyArray(maksedKey, maskedByte);

        int[] aggSurrogateKey = new int[surrogateIndex.length];

        for (int i = 0; i < aggSurrogateKey.length; i++) {
            aggSurrogateKey[i] = (int) keyArray[surrogateIndex[i]];
        }

        try {
            return keyGenerator.generateKey(aggSurrogateKey);
        } catch (KeyGenException e) {
            throw new MolapDataWriterException("Problem while generating the mdkeyfor aggregate ",
                    e);
        }
    }

    /**
     * below method will be used to close the handler
     */
    public void closeHandler() {
        if (null != this.dataWriter) {
            // close all the open stream for both the files
            this.dataWriter.closeChannle();
            int size = fileManager.size();
            FileData fileData = null;
            String storePath = null;
            String inProgFileName = null;
            String changedFileName = null;
            File currentFile = null;
            File destFile = null;
            for (int i = 0; i < size; i++) {
                fileData = (FileData) fileManager.get(i);

                storePath = fileData.getStorePath();
                inProgFileName = fileData.getFileName();
                changedFileName = inProgFileName.substring(0, inProgFileName.lastIndexOf('.'));
                currentFile = new File(storePath + File.separator + inProgFileName);
                destFile = new File(storePath + File.separator + changedFileName);
                if (!currentFile.renameTo(destFile)) {
                    LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                            "Problem while renaming the file");
                }
                fileData.setName(changedFileName);
            }
        }
        if (null != groupBy) {
            try {
                this.groupBy.finish();
            } catch (MolapGroupByException e) {
                LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Problem while closing the groupby file");
            }
        }
        this.keyStore = null;
        this.dataStore = null;
        this.dataWriter = null;
        this.groupBy = null;
    }

    /**
     * Below method will be to configure fact file writing configuration
     *
     * @throws MolapDataWriterException
     */
    private void setWritingConfiguration(int mdkeySize) throws MolapDataWriterException {
        String measureMetaDataFileLocation =
                this.storeLocation + MolapCommonConstants.MEASURE_METADATA_FILE_NAME
                        + this.tableName + MolapCommonConstants.MEASUREMETADATA_FILE_EXT;
        // get the compression model
        // this will used max, min and decimal point value present in the
        // and the measure count to get the compression for each measure
        this.compressionModel = ValueCompressionUtil
                .getValueCompressionModel(measureMetaDataFileLocation, this.measureCount);
        this.uniqueValue = compressionModel.getUniqueValue();
        // get leaf node size
        this.leafNodeSize = Integer.parseInt(MolapProperties.getInstance()
                .getProperty(MolapCommonConstants.LEAFNODE_SIZE,
                        MolapCommonConstants.LEAFNODE_SIZE_DEFAULT_VAL));

        // create key store
        this.keyStore = StoreFactory.createKeyStore(this.leafNodeSize, mdkeySize, true);

        // create data store
        this.dataStore = StoreFactory.createDataStore(compressionModel);
        // agg type
        char[] type = compressionModel.getType();
        List<Integer> otherMeasureIndexLst =
                new ArrayList<Integer>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        List<Integer> customMeasureIndexLst =
                new ArrayList<Integer>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        for (int j = 0; j < type.length; j++) {
            if (type[j] != 'c') {
                otherMeasureIndexLst.add(j);
            } else {
                customMeasureIndexLst.add(j);
            }
        }
        otherMeasureIndex = new int[otherMeasureIndexLst.size()];
        customMeasureIndex = new int[customMeasureIndexLst.size()];
        for (int i = 0; i < otherMeasureIndex.length; i++) {
            otherMeasureIndex[i] = otherMeasureIndexLst.get(i);
        }
        for (int i = 0; i < customMeasureIndex.length; i++) {
            customMeasureIndex[i] = customMeasureIndexLst.get(i);
        }
        initialisedataHolder();
        // create data writer instance
        this.dataWriter = new MolapDataWriter(this.storeLocation, this.measureCount, mdkeySize,
                this.tableName, true);
        this.dataWriter.setFileManager(fileManager);
        // initialize the channel;
        this.dataWriter.initChannel();
    }

    private void initialisedataHolder() {
        this.dataHolder = new MolapWriteDataHolder[this.measureCount];

        for (int i = 0; i < otherMeasureIndex.length; i++) {
            this.dataHolder[otherMeasureIndex[i]] = new MolapWriteDataHolder();
            this.dataHolder[otherMeasureIndex[i]].initialiseDoubleValues(this.leafNodeSize);
        }
        for (int i = 0; i < customMeasureIndex.length; i++) {
            this.dataHolder[customMeasureIndex[i]] = new MolapWriteDataHolder();
            this.dataHolder[customMeasureIndex[i]].initialiseByteArrayValues(leafNodeSize);
        }
    }
}
