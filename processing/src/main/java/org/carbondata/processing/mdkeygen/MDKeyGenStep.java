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

package org.carbondata.processing.mdkeygen;

import java.io.File;
import java.util.*;
import java.util.Map.Entry;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.common.logging.impl.StandardLogService;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.file.manager.composite.FileData;
import org.carbondata.core.file.manager.composite.IFileManagerComposite;
import org.carbondata.core.file.manager.composite.LoadFolderData;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.carbondata.core.util.*;
import org.carbondata.core.vo.HybridStoreModel;
import org.carbondata.processing.datatypes.GenericDataType;
import org.carbondata.processing.store.CarbonFactDataHandlerColumnar;
import org.carbondata.processing.store.CarbonFactHandler;
import org.carbondata.processing.store.SingleThreadFinalSortFilesMerger;
import org.carbondata.processing.store.writer.exception.CarbonDataWriterException;
import org.carbondata.processing.util.CarbonDataProcessorLogEvent;
import org.carbondata.processing.util.RemoveDictionaryUtil;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

public class MDKeyGenStep extends BaseStep {
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(MDKeyGenStep.class.getName());

    /**
     * carbon mdkey generator step data class
     */
    private MDKeyGenStepData data;

    /**
     * carbon mdkey generator step meta
     */
    private MDKeyGenStepMeta meta;

    /**
     * dimension length
     */
    private int dimensionCount;

    /**
     * table name
     */
    private String tableName;

    /**
     * File manager
     */
    private IFileManagerComposite fileManager;

    private Map<Integer, GenericDataType> complexIndexMap;

    /**
     * readCounter
     */
    private long readCounter;

    /**
     * writeCounter
     */
    private long writeCounter;

    private int measureCount;

    private String dataFolderLocation;

    private SingleThreadFinalSortFilesMerger finalMerger;

    /**
     * dataHandler
     */
    private CarbonFactHandler dataHandler;

    private char[] aggType;

    private String storeLocation;

    private int[] dimLens;

    private HybridStoreModel hybridStoreModel;

    /**
     * CarbonMDKeyGenStep
     *
     * @param stepMeta
     * @param stepDataInterface
     * @param copyNr
     * @param transMeta
     * @param trans
     */
    public MDKeyGenStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
            TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    /**
     * Perform the equivalent of processing one row. Typically this means
     * reading a row from input (getRow()) and passing a row to output
     * (putRow)).
     *
     * @param smi The steps metadata to work with
     * @param sdi The steps temporary working data to work with (database
     *            connections, result sets, caches, temporary variables, etc.)
     * @return false if no more rows can be processed or an error occurred.
     * @throws KettleException
     */
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        meta = (MDKeyGenStepMeta) smi;
        StandardLogService
                .setThreadName(StandardLogService.getPartitionID(meta.getCubeName()), null);
        data = (MDKeyGenStepData) sdi;

        meta.initialize();
        Object[] row = getRow();
        if (first) {
            first = false;

            data.outputRowMeta = new RowMeta();
            boolean isExecutionRequired = setStepConfiguration();

            if (!isExecutionRequired) {
                processingComplete();
                return false;
            }
            setStepOutputInterface();
        }

        readCounter++;

        if (null != row) {
            putRow(data.outputRowMeta, new Object[measureCount + 1]);
            return true;
        }

        try {
            initDataHandler();
            dataHandler.initialise();
            finalMerger.startFinalMerge();
            while (finalMerger.hasNext()) {
                Object[] r = finalMerger.next();
                Object[] outputRow = process(r);
                dataHandler.addDataToStore(outputRow);
                writeCounter++;
            }
        } catch (CarbonDataWriterException e) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, e,
                    "Failed for: " + this.tableName);
            throw new KettleException("Error while initializing data handler : " + e.getMessage());
        } finally {
            try {
                dataHandler.finish();
            } catch (CarbonDataWriterException e) {
                LOGGER.debug(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                        "Error in  closing data handler ");
            }
        }
        LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                "Record Procerssed For table: " + this.tableName);
        String logMessage =
                "Finished Carbon Mdkey Generation Step: Read: " + readCounter + ": Write: "
                        + writeCounter;
        LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, logMessage);
        processingComplete();
        return false;
    }

    private void processingComplete() {
        if (null != dataHandler) {
            dataHandler.closeHandler();
        }
        setOutputDone();
    }

    /**
     * This method will be used to get and update the step properties which will
     * required to run this step
     *
     * @throws CarbonUtilException
     */
    private boolean setStepConfiguration() {
        this.tableName = meta.getTableName();
        CarbonProperties instance = CarbonProperties.getInstance();
        String tempLocationKey = meta.getSchemaName() + '_' + meta.getCubeName();
        String baseStorelocation = instance.getProperty(tempLocationKey,
                CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL) + File.separator + meta
                .getSchemaName() + File.separator + meta.getCubeName();

        int restructFolderNumber = meta.getCurrentRestructNumber()/*CarbonUtil.checkAndReturnNextRestructFolderNumber(baseStorelocation,"RS_")*/;

        String restructFolderlocation =
                baseStorelocation + File.separator + CarbonCommonConstants.RESTRUCTRE_FOLDER
                        + restructFolderNumber + File.separator + this.tableName;

        int counter = CarbonUtil.checkAndReturnCurrentLoadFolderNumber(restructFolderlocation);

        // This check is just to get the absolute path because from the property file Relative path 
        // will come and sometimes FileOutPutstream was not able to Create the file.
        File file = new File(restructFolderlocation);
        storeLocation = file.getAbsolutePath() + File.separator + CarbonCommonConstants.LOAD_FOLDER
                + counter;

        fileManager = new LoadFolderData();
        fileManager.setName(CarbonCommonConstants.LOAD_FOLDER + counter
                + CarbonCommonConstants.FILE_INPROGRESS_STATUS);

        storeLocation = storeLocation + CarbonCommonConstants.FILE_INPROGRESS_STATUS;

        if (!(new File(storeLocation).exists())) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                    "Load Folder Not Present for writing measure metadata  : " + storeLocation);
            return false;
        }

        this.meta.setNoDictionaryCount(
                RemoveDictionaryUtil.extractNoDictionaryCount(this.meta.getNoDictionaryDims()));

        String levelCardinalityFilePath = storeLocation + File.separator +
                CarbonCommonConstants.LEVEL_METADATA_FILE + meta.getTableName() + ".metadata";

        int[] dimLensWithComplex=null;
        try{
          dimLensWithComplex =
                    CarbonUtil.getCardinalityFromLevelMetadataFile(levelCardinalityFilePath);
        }catch(CarbonUtilException e){
          LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                    "Level cardinality file :: " + e.getMessage());
                 return false;
        }
        if(null==dimLensWithComplex){
          return false;
        }
        List<Integer> dimsLenList = new ArrayList<Integer>();
        for (int eachDimLen : dimLensWithComplex) {
            if (eachDimLen != 0) dimsLenList.add(eachDimLen);
        }
        dimLens = new int[dimsLenList.size()];
        for (int i = 0; i < dimsLenList.size(); i++) {
            dimLens[i] = dimsLenList.get(i);
        }
 
        String[] dimStoreType = meta.getDimensionsStoreType().split(",");
        boolean[] dimensionStoreType = new boolean[dimLens.length];
        for (int i = 0; i < dimensionStoreType.length; i++) {
            dimensionStoreType[i] = Boolean.parseBoolean(dimStoreType[i]);
        }
        this.hybridStoreModel = CarbonUtil.getHybridStoreMeta(dimLens, dimensionStoreType, null);
        dimLens = hybridStoreModel.getHybridCardinality();
        data.generator = new KeyGenerator[dimLens.length + 1];
        for (int i = 0; i < dimLens.length; i++) {
            data.generator[i] = KeyGeneratorFactory.getKeyGenerator(new int[] { dimLens[i] });
        }

        //      this.dimensionCount = dimLens.length;
        this.dimensionCount = meta.getDimensionCount();

        int simpleDimsCount =
                this.dimensionCount - meta.getComplexDimsCount() - meta.getNoDictionaryCount();
        int[] simpleDimsLen = new int[simpleDimsCount];
        for (int i = 0; i < simpleDimsCount; i++) {
            simpleDimsLen[i] = dimLens[i];
        }

        //Actual primitive dimension used to generate start & end key

        //data.generator[dimLens.length] = KeyGeneratorFactory.getKeyGenerator(simpleDimsLen);
        data.generator[dimLens.length] = KeyGeneratorFactory
                .getKeyGenerator(hybridStoreModel.getHybridCardinality(),
                        hybridStoreModel.getDimensionPartitioner());

        //To Set MDKey Index of each primitive type in complex type
        int surrIndex = simpleDimsCount;
        Iterator<Entry<String, GenericDataType>> complexMap =
                meta.getComplexTypes().entrySet().iterator();
        complexIndexMap = new HashMap<Integer, GenericDataType>(meta.getComplexDimsCount());
        while (complexMap.hasNext()) {
            Entry<String, GenericDataType> complexDataType = complexMap.next();
            complexDataType.getValue().setOutputArrayIndex(0);
            complexIndexMap.put(simpleDimsCount, complexDataType.getValue());
            simpleDimsCount++;
            List<GenericDataType> primitiveTypes = new ArrayList<GenericDataType>();
            complexDataType.getValue().getAllPrimitiveChildren(primitiveTypes);
            for (GenericDataType eachPrimitive : primitiveTypes) {
                eachPrimitive.setSurrogateIndex(surrIndex++);
            }
        }

        this.measureCount = meta.getMeasureCount();

        String metaDataFileName = CarbonCommonConstants.MEASURE_METADATA_FILE_NAME + this.tableName
                + CarbonCommonConstants.MEASUREMETADATA_FILE_EXT
                + CarbonCommonConstants.FILE_INPROGRESS_STATUS;

        FileData fileData = new FileData(metaDataFileName, storeLocation);
        fileManager.add(fileData);

        // Set the data file location
        this.dataFolderLocation = baseStorelocation + File.separator +
                CarbonCommonConstants.SORT_TEMP_FILE_LOCATION + File.separator + this.tableName;
        return true;
    }

    private void initDataHandler() {
        ValueCompressionModel valueCompressionModel = getValueCompressionModel(storeLocation);
        int simpleDimsCount =
                this.dimensionCount - meta.getComplexDimsCount() - meta.getNoDictionaryCount();
        int[] simpleDimsLen = new int[simpleDimsCount];
        for (int i = 0; i < simpleDimsCount; i++) {
            simpleDimsLen[i] = dimLens[i];
        }
        String measureDataType = meta.getMeasureDataType();
        String[] msrdataTypes = null;
        if (measureDataType.length() > 0) {
            msrdataTypes = measureDataType.split(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);
        } else {
            msrdataTypes = new String[0];
        }

        //aggType = valueCompressionModel.getType();
        initAggType(msrdataTypes);
        finalMerger = new SingleThreadFinalSortFilesMerger(dataFolderLocation, tableName,
                dimensionCount - meta.getComplexDimsCount(), meta.getComplexDimsCount(),
                measureCount, meta.getNoDictionaryCount(), aggType);
        if (meta.getNoDictionaryCount() > 0) {
            dataHandler = new CarbonFactDataHandlerColumnar(meta.getSchemaName(), meta.getCubeName(),
                    this.tableName, false, measureCount,
                    data.generator[dimLens.length].getKeySizeInBytes(), measureCount + 1, null,
                    null, storeLocation, dimLens, false, false, dimLens, null, null, true,
                    meta.getCurrentRestructNumber(), meta.getNoDictionaryCount(), dimensionCount,
                    complexIndexMap, simpleDimsLen, this.hybridStoreModel, aggType);
        } else {
            dataHandler = new CarbonFactDataHandlerColumnar(meta.getSchemaName(), meta.getCubeName(),
                    this.tableName, false, measureCount,
                    data.generator[dimLens.length].getKeySizeInBytes(), measureCount, null, null,
                    storeLocation, dimLens, false, false, dimLens, null, null, true,
                    meta.getCurrentRestructNumber(), meta.getNoDictionaryCount(), dimensionCount,
                    complexIndexMap, simpleDimsLen, this.hybridStoreModel, aggType);
        }
    }

    private ValueCompressionModel getValueCompressionModel(String storeLocation) {
        String measureMetaDataFileLoc =
                storeLocation + CarbonCommonConstants.MEASURE_METADATA_FILE_NAME + this.tableName
                        + CarbonCommonConstants.MEASUREMETADATA_FILE_EXT;
        return ValueCompressionUtil
                .getValueCompressionModel(measureMetaDataFileLoc, this.measureCount);
    }

    private void initAggType(String[] msrdataTypes) {
        aggType = new char[measureCount];
        Arrays.fill(aggType, 'n');
        for (int i = 0; i < measureCount; i++) {
            aggType[i] = DataTypeUtil.getAggType(msrdataTypes[i]);
        }
    }

    /**
     * This method will be used for setting the output interface.
     * Output interface is how this step will process the row to next step
     */
    private void setStepOutputInterface() {
        ValueMetaInterface[] out = new ValueMetaInterface[measureCount + 1];

        for (int i = 0; i < measureCount; i++) {
            out[i] = new ValueMeta("measure" + i, ValueMetaInterface.TYPE_NUMBER,
                    ValueMetaInterface.STORAGE_TYPE_NORMAL);
            out[i].setStorageMetadata(new ValueMeta("measure" + i, ValueMetaInterface.TYPE_NUMBER,
                    ValueMetaInterface.STORAGE_TYPE_NORMAL));
        }

        out[out.length - 1] = new ValueMeta("id", ValueMetaInterface.TYPE_BINARY,
                ValueMetaInterface.STORAGE_TYPE_BINARY_STRING);
        out[out.length - 1].setStorageMetadata(new ValueMeta("id", ValueMetaInterface.TYPE_STRING,
                ValueMetaInterface.STORAGE_TYPE_NORMAL));
        out[out.length - 1].setLength(256);
        out[out.length - 1].setStringEncoding(CarbonCommonConstants.BYTE_ENCODING);
        out[out.length - 1].getStorageMetadata()
                .setStringEncoding(CarbonCommonConstants.BYTE_ENCODING);

        data.outputRowMeta.setValueMetaList(Arrays.asList(out));
    }

    /**
     * This method will be used to get the row from previous step and then it
     * will generate the mdkey and then send the mdkey to next step
     *
     * @param row input row
     * @throws KettleException
     */
    private Object[] process(Object[] row) throws KettleException {
        Object[] outputRow = null;
        // adding one for the high cardinality dims byte array.
        if (meta.getNoDictionaryCount() > 0 || meta.getComplexDimsCount() > 0) {
            outputRow = new Object[measureCount + 1 + 1];
        } else {
            outputRow = new Object[measureCount + 1];
        }
        int[] keys = new int[this.dimensionCount];

        int l = 0;
        int index = 0;
        for (int i = 0; i < measureCount; i++) {
            if (aggType[i] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
                outputRow[l++] = RemoveDictionaryUtil.getMeasure(index++, row);
            } else if (aggType[i] == CarbonCommonConstants.BIG_INT_MEASURE) {
                outputRow[l++] = (Long) RemoveDictionaryUtil.getMeasure(index++, row);
            } else {
                outputRow[l++] = (Double) RemoveDictionaryUtil.getMeasure(index++, row);
            }
        }
        outputRow[l] = RemoveDictionaryUtil.getByteArrayForNoDictionaryCols(row);

        //copy all columnar dimension to key array
        int[] columnarStoreOrdinals = hybridStoreModel.getColumnStoreOrdinals();
        int[] columnarDataKeys = new int[columnarStoreOrdinals.length];
        for (int i = 0; i < columnarStoreOrdinals.length; i++) {
            Object key = RemoveDictionaryUtil.getDimension(columnarStoreOrdinals[i], row);
            columnarDataKeys[i] = (Integer) key;
        }
        //copy all row dimension in row key array
        int[] rowStoreOrdinals = hybridStoreModel.getRowStoreOrdinals();
        int[] rowDataKeys = new int[rowStoreOrdinals.length];
        for (int i = 0; i < rowStoreOrdinals.length; i++) {
            Object key = RemoveDictionaryUtil.getDimension(rowStoreOrdinals[i], row);
            rowDataKeys[i] = (Integer) key;
        }
        try {
            int[] completeKeys = new int[columnarDataKeys.length + rowDataKeys.length];
            System.arraycopy(rowDataKeys, 0, completeKeys, 0, rowDataKeys.length);
            System.arraycopy(columnarDataKeys, 0, completeKeys, rowDataKeys.length,
                    columnarDataKeys.length);
            outputRow[outputRow.length - 1] =
                    data.generator[data.generator.length - 1].generateKey(completeKeys);
        } catch (KeyGenException e) {
            throw new KettleException("Unbale to generate the mdkey", e);
        }

        return outputRow;
    }

    /**
     * Initialize and do work where other steps need to wait for...
     *
     * @param smi The metadata to work with
     * @param sdi The data to initialize
     * @return step initialize or not
     */
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (MDKeyGenStepMeta) smi;
        data = (MDKeyGenStepData) sdi;

        return super.init(smi, sdi);
    }

    /**
     * Dispose of this step: close files, empty logs, etc.
     *
     * @param smi The metadata to work with
     * @param sdi The data to dispose of
     */
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (MDKeyGenStepMeta) smi;
        data = (MDKeyGenStepData) sdi;
        super.dispose(smi, sdi);
        dataHandler = null;
        finalMerger = null;
    }

}
