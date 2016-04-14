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

package org.carbondata.processing.factreader.step;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.CarbonDef;
import org.carbondata.core.carbon.CarbonDef.AggLevel;
import org.carbondata.core.carbon.CarbonDef.AggMeasure;
import org.carbondata.core.carbon.CarbonDef.Schema;
import org.carbondata.core.carbon.SqlStatement;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.columnar.impl.MultiDimKeyVarLengthEquiSplitGenerator;
import org.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.carbondata.core.metadata.CarbonMetadata;
import org.carbondata.core.metadata.CarbonMetadata.Cube;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.core.metadata.CarbonMetadata.Measure;
import org.carbondata.core.metadata.SliceMetaData;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.processing.util.CarbonDataProcessorLogEvent;
import org.carbondata.processing.util.CarbonSchemaParser;
import org.carbondata.processing.util.RemoveDictionaryUtil;
import org.carbondata.query.aggregator.CustomCarbonAggregateExpression;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.aggregator.dimension.DimensionAggregatorInfo;
import org.carbondata.query.cache.QueryExecutorUtil;
import org.carbondata.query.datastorage.InMemoryLoadTableUtil;
import org.carbondata.query.datastorage.InMemoryTable;
import org.carbondata.query.datastorage.InMemoryTableStore;
import org.carbondata.query.executer.SliceExecuter;
import org.carbondata.query.executer.exception.QueryExecutionException;
import org.carbondata.query.executer.impl.ColumnarParallelSliceExecutor;
import org.carbondata.query.executer.impl.RestructureHolder;
import org.carbondata.query.executer.impl.RestructureUtil;
import org.carbondata.query.executer.pagination.impl.QueryResult;
import org.carbondata.query.executer.pagination.impl.QueryResult.QueryResultIterator;
import org.carbondata.query.schema.metadata.SliceExecutionInfo;
import org.carbondata.query.util.QueryExecutorUtility;
import org.carbondata.query.wrappers.ByteArrayWrapper;
import org.eigenbase.xom.Parser;
import org.eigenbase.xom.XOMUtil;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

public class CarbonFactReaderStep extends BaseStep implements StepInterface {

    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(CarbonFactReaderStep.class.getName());

    /**
     * BYTE ENCODING
     */
    private static final String BYTE_ENCODING = "ISO-8859-1";

    /**
     * lock
     */
    private static final Object LOCK = new Object();
    /**
     * array of sql datatypes of mesaures and dimensions
     */
    protected SqlStatement.Type[] dataTypes;
    protected HashMap<Integer, Integer> measureOrdinalMap = new HashMap<>();
    List<DimensionAggregatorInfo> dimAggInfo;
    /**
     * meta
     */
    private CarbonFactReaderMeta meta;
    /**
     * data
     */
    private CarbonFactReaderData data;
    /**
     * writeCounter
     */
    private long writeCounter;
    /**
     * logCounter
     */
    private int logCounter;
    /**
     * executorService
     */
    private ExecutorService executorService;
    /**
     * threadStatusObserver
     */
    private ThreadStatusObserver threadStatusObserver;
    /**
     * readCopies
     */
    private int readCopies;
    /**
     * aggregate dimensions array
     */
    private AggLevel[] aggLevels;
    /**
     * aggregate measures array
     */
    private AggMeasure[] aggMeasures;
    private Dimension[] currentQueryDims;
    private Measure[] currentQueryMeasures;
    private String[] aggTypes;
    /**
     * this is used to store the mapping of high card dims along with agg types.
     */
    private boolean[] isNoDictionary;

    /**
     * CarbonFactReaderStep Constructor to initialize the step
     *
     * @param stepMeta
     * @param stepDataInterface
     * @param copyNr
     * @param transMeta
     * @param trans
     */
    public CarbonFactReaderStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
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
        try {
            if (first) {
                LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                        "File Based fact reader will be used For " + meta.getTableName());
                meta = (CarbonFactReaderMeta) smi;
                data = (CarbonFactReaderData) sdi;
                data.outputRowMeta = new RowMeta();
                first = false;
                meta.initialize();
                this.logCounter = Integer.parseInt(
                        CarbonCommonConstants.DATA_LOAD_LOG_COUNTER_DEFAULT_COUNTER);
                setStepOutputInterface(meta.getAggType());
                try {
                    readCopies = Integer.parseInt(CarbonProperties.getInstance()
                            .getProperty(CarbonCommonConstants.NUM_CORES_LOADING,
                                    CarbonCommonConstants.DEFAULT_NUMBER_CORES));
                } catch (NumberFormatException e) {
                    LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                            "Invalid Value for: " + CarbonCommonConstants.NUM_CORES_LOADING
                                    + "Default Value: " + CarbonCommonConstants.DEFAULT_NUMBER_CORES
                                    + " will be used");
                    readCopies = Integer.parseInt(CarbonCommonConstants.DEFAULT_NUMBER_CORES);
                }
                this.executorService = Executors.newFixedThreadPool(readCopies);
                this.threadStatusObserver = new ThreadStatusObserver();
                initAggTable();
            }
            Callable<Void> c = new DataProcessor();
            this.executorService.submit(c);
            this.executorService.shutdown();
            this.executorService.awaitTermination(1, TimeUnit.DAYS);
            LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                    "Record Procerssed For table: " + meta.getTableName());
            String logMessage =
                    "Summary: Carbon Fact Reader Step: Read: " + 0 + ": Write: " + writeCounter;
            LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, logMessage);
        } catch (Exception ex) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, ex);
            throw new RuntimeException(ex);
        }
        setOutputDone();
        return false;
    }

    private Schema parseStringToSchema(String schema) throws Exception {
        Parser xmlParser = XOMUtil.createDefaultParser();
        ByteArrayInputStream baoi =
                new ByteArrayInputStream(schema.getBytes(Charset.defaultCharset()));
        return new CarbonDef.Schema(xmlParser.parse(baoi));
    }

    private void initAggTable() throws Exception {
        CarbonDef.Schema schema = parseStringToSchema(meta.getSchema());
        CarbonDef.Cube cube = CarbonSchemaParser.getMondrianCube(schema, meta.getCubeName());
        CarbonDef.Table table = (CarbonDef.Table) cube.fact;
        CarbonDef.AggTable[] aggTables = table.aggTables;
        int numberOfAggregates = aggTables.length;
        for (int i = 0; i < numberOfAggregates; i++) {
            String aggTableName = ((CarbonDef.AggName) aggTables[i]).getNameAttribute();
            if (aggTableName.equals(meta.getAggregateTableName())) {
                aggLevels = aggTables[i].getAggLevels();
                aggMeasures = aggTables[i].getAggMeasures();
                break;
            }
        }
    }

    /**
     * @return
     */
    private String getLoadName() {
        String location = meta.getFactStoreLocation();
        String loadName =
                location.substring(location.lastIndexOf(CarbonCommonConstants.LOAD_FOLDER));
        return loadName;
    }

    private Dimension[] getSelectedQueryDimensions(String[] dims, Dimension[] currentDimTables,
            String tableName) {
        List<Dimension> selectedQueryDimensions =
                new ArrayList<Dimension>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        for (int i = 0; i < dims.length; i++) {
            for (int j = 0; j < currentDimTables.length; j++) {
                if (dims[i].equals(tableName + '_' + currentDimTables[j].getName())) {
                    selectedQueryDimensions.add(currentDimTables[j]);
                    break;
                }
            }
        }
        return selectedQueryDimensions.toArray(new Dimension[selectedQueryDimensions.size()]);
    }

    private SliceExecutionInfo getSliceExecutionInfo(Cube cube, List<InMemoryTable> activeSlices,
            InMemoryTable slice, SliceMetaData sliceMataData, int currentSliceIndex)
            throws KeyGenException, QueryExecutionException {
        RestructureHolder holder = new RestructureHolder();
        KeyGenerator globalKeyGenerator = null;
        Dimension[] queryDimensions =
                getSelectedQueryDimensions(sliceMataData.getDimensions(), currentQueryDims,
                        cube.getFactTableName());
        globalKeyGenerator = KeyGeneratorFactory.getKeyGenerator(meta.getGlobalDimLens());
        if (!globalKeyGenerator.equals(slice.getKeyGenerator(cube.getFactTableName()))) {
            holder.updateRequired = true;
        }

        holder.metaData = sliceMataData;
        int[] measureOrdinal = new int[currentQueryMeasures.length];
        boolean[] msrExists = new boolean[currentQueryMeasures.length];
        Object[] newMsrsDftVal = new Object[currentQueryMeasures.length];
        RestructureUtil
                .updateMeasureInfo(sliceMataData, currentQueryMeasures, measureOrdinal, msrExists,
                        newMsrsDftVal);
        SliceExecutionInfo info = new SliceExecutionInfo();
        info.setDimensions(currentQueryDims);
        info.setIsMeasureExistis(msrExists);
        info.setMsrDefaultValue(newMsrsDftVal);
        Object[] sliceUniqueValues = null;
        boolean isCustomMeasure = false;
        sliceUniqueValues = slice.getDataCache(cube.getFactTableName()).getUniqueValue();
        char[] type = slice.getDataCache(cube.getFactTableName()).getType();
        for (int i = 0; i < type.length; i++) {
            if (type[i] == 'c') {
                isCustomMeasure = true;
            }
        }
        info.setCustomMeasure(isCustomMeasure);
        info.setTableName(cube.getFactTableName());
        info.setKeyGenerator(slice.getKeyGenerator(cube.getFactTableName()));
        holder.setKeyGenerator(slice.getKeyGenerator(cube.getFactTableName()));
        info.setQueryDimensions(queryDimensions);
        info.setMeasureOrdinal(measureOrdinal);
        info.setCubeName(meta.getCubeName());
        info.setSchemaName(meta.getSchemaName());
        info.setQueryId(System.currentTimeMillis() + "");
        info.setDetailQuery(false);
        int[] maskByteRanges =
                QueryExecutorUtil.getMaskedByte(queryDimensions, globalKeyGenerator, null);
        info.setMaskedKeyByteSize(maskByteRanges.length);
        int[] maskedBytesLocal =
                new int[slice.getKeyGenerator(cube.getFactTableName()).getKeySizeInBytes()];
        QueryExecutorUtil.updateMaskedKeyRanges(maskedBytesLocal, maskByteRanges);
        holder.maskedByteRanges = maskedBytesLocal;
        // creating a masked key
        int[] maskedBytes = new int[globalKeyGenerator.getKeySizeInBytes()];
        // update the masked byte
        QueryExecutorUtil.updateMaskedKeyRanges(maskedBytes, maskByteRanges);
        info.setMaskedBytePositions(maskedBytes);

        // get the mask byte range based on dimension present in the query
        maskByteRanges =
                QueryExecutorUtil.getMaskedByte(currentQueryDims, globalKeyGenerator, null);

        // update the masked byte
        QueryExecutorUtil.updateMaskedKeyRanges(maskedBytes, maskByteRanges);
        info.setActalMaskedByteRanges(maskByteRanges);
        info.setMaskedBytePositions(maskedBytes);
        info.setActualMaskedKeyByteSize(maskByteRanges.length);
        info.setActualMaskedKeyByteSize(maskByteRanges.length);
        List<Dimension> dimList = cube.getDimensions(cube.getFactTableName());
        Dimension[] dimemsions = null;
        if (dimList != null) {
            dimemsions = dimList.toArray(new Dimension[dimList.size()]);
        }
        info.setActualMaxKeyBasedOnDimensions(QueryExecutorUtil
                .getMaxKeyBasedOnDimensions(currentQueryDims, globalKeyGenerator, dimemsions));
        info.setActualKeyGenerator(globalKeyGenerator);
        info.setRestructureHolder(holder);
        info.setSlice(slice);
        info.setSlices(activeSlices);
        info.setAvgIndexes(new ArrayList<Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE));
        info.setCountMsrsIndex(-1);
        info.setDimensionSortOrder(new byte[0]);
        info.setUniqueValues(sliceUniqueValues);
        info.setOriginalDims(queryDimensions);
        int[][] maskedByteRangeForSorting = QueryExecutorUtility
                .getMaskedByteRangeForSorting(queryDimensions, globalKeyGenerator, maskByteRanges);
        info.setMaskedByteRangeForSorting(maskedByteRangeForSorting);
        info.setDimensionMaskKeys(QueryExecutorUtility
                .getMaksedKeyForSorting(queryDimensions, globalKeyGenerator,
                        maskedByteRangeForSorting, maskByteRanges));
        info.setColumnarSplitter(new MultiDimKeyVarLengthEquiSplitGenerator(CarbonUtil
                .getIncrementedCardinalityFullyFilled(
                        slice.getDataCache(cube.getFactTableName()).getDimCardinality()),
                (byte) 1));
        info.setQueryDimOrdinal(QueryExecutorUtility.getSelectedDimnesionIndex(queryDimensions));
        List<Dimension> customDim =
                new ArrayList<Dimension>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        info.setAllSelectedDimensions(QueryExecutorUtility
                .getAllSelectedDiemnsion(queryDimensions, dimAggInfo, customDim));
        info.setLimit(-1);
        info.setTotalNumerOfDimColumns(cube.getDimensions(cube.getFactTableName()).size());
        info.setTotalNumberOfMeasuresInTable(cube.getMeasures(cube.getFactTableName()).size());

        long[] startKey = new long[cube.getDimensions(meta.getTableName()).size()];
        long[] endKey = meta.getDimLens();
        info.setStartKey(startKey);
        info.setEndKey(endKey);
        info.setNumberOfRecordsInMemory(Integer.MAX_VALUE);
        info.setOutLocation(CarbonProperties.getInstance()
                .getProperty(CarbonCommonConstants.STORE_LOCATION_HDFS,
                        CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL));
        info.setDimAggInfo(dimAggInfo);
        info.setCurrentSliceIndex(currentSliceIndex);
        int measureStartIndex = aggTypes.length - currentQueryMeasures.length;
        // check for is aggregate table here as second parameter below is isAggTable boolean (table from which data has to be read)
        Object[] msrMinValue = QueryExecutorUtility
                .getMinValueOfSlices(cube.getFactTableName(), false, activeSlices, dataTypes);
        Object[] queryMsrMinValue = new Object[aggTypes.length];
        for (int i = 0; i < currentQueryMeasures.length; i++) {
            queryMsrMinValue[measureStartIndex + i] =
                    msrMinValue[currentQueryMeasures[i].getOrdinal()];
        }
        info.setMsrMinValue(queryMsrMinValue);
        info.setAggType(aggTypes);
        info.setMeasureStartIndex(measureStartIndex);
        List<CustomCarbonAggregateExpression> expression =
                new ArrayList<CustomCarbonAggregateExpression>(
                        CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        info.setCustomExpressions(expression);
        info.setDataTypes(dataTypes);
        info.setMeasureOrdinalMap(measureOrdinalMap);
        return info;
    }

    private void initQueryDims(Cube cube) {
        currentQueryDims = new Dimension[aggLevels.length];
        int i = 0;
        for (AggLevel aggLevel : aggLevels) {
            currentQueryDims[i++] = cube.getDimension(aggLevel.column, cube.getFactTableName());
        }
    }

    private String[] getAggTypes() {
        String[] aggTypes = new String[aggMeasures.length];
        // initializing high card mapping.
        isNoDictionary = new boolean[aggTypes.length];

        // executerProperties.a
        for (int i = 0; i < aggMeasures.length; i++) {
            if (null != dimAggInfo) {
                for (DimensionAggregatorInfo dimAgg : dimAggInfo) {
                    // checking if the dimension aggregate is high cardinality or not.
                    if (aggMeasures[i].column.equals(dimAgg.getColumnName()) && dimAgg.getDim()
                            .isNoDictionaryDim()) {
                        isNoDictionary[i] = true;
                    }
                }
            }
            aggTypes[i] = aggMeasures[i].aggregator;
        }
        return aggTypes;
    }

    /**
     * @param cube
     */
    private void initQueryMsrs(Cube cube) {
        int count = 0;
        int[] matchedIndexes = new int[aggMeasures.length];
        Measure[] measure = new Measure[aggMeasures.length];
        dataTypes = new SqlStatement.Type[aggMeasures.length];
        int typeCount = 0;
        for (int i = 0; i < aggMeasures.length; i++) {
            measure[i] = cube.getMeasure(meta.getTableName(), aggMeasures[i].column);
            if (null != measure[i]) {
                measure[i].setAggName(aggMeasures[i].aggregator);
                matchedIndexes[count++] = i;
                measureOrdinalMap.put(measure[i].getOrdinal(), i);
            } else {
                Dimension dimension =
                        cube.getDimension(aggMeasures[i].column, cube.getFactTableName());
                DimensionAggregatorInfo info = new DimensionAggregatorInfo();
                List<String> aggList = new ArrayList<String>(1);
                aggList.add(aggMeasures[i].aggregator);
                info.setAggList(aggList);
                if (dimension != null) {
                    info.setColumnName(dimension.getColName());
                    info.setDim(dimension);
                    dataTypes[typeCount++] = dimension.getDataType();
                }
                dimAggInfo.add(info);
            }
        }
        currentQueryMeasures = new Measure[count];
        for (int i = 0; i < count; i++) {
            currentQueryMeasures[i] = measure[matchedIndexes[i]];
            dataTypes[typeCount++] = currentQueryMeasures[i].getDataType();
        }
    }

    /**
     * This method will be used for setting the output interface. Output
     * interface is how this step will process the row to next step
     */
    private void setStepOutputInterface(String[] aggType) {
        int size = 1 + this.meta.getMeasureCount();
        ValueMetaInterface[] out = new ValueMetaInterface[size];
        int l = 0;
        final String measureConst = "measure";
        ValueMetaInterface valueMetaInterface = null;
        ValueMetaInterface storageMetaInterface = null;
        int measureCount = meta.getMeasureCount();
        for (int i = 0; i < measureCount; i++) {
            if (aggType[i].charAt(0) == 'n') {
                valueMetaInterface = new ValueMeta(measureConst + i, ValueMetaInterface.TYPE_NUMBER,
                        ValueMetaInterface.STORAGE_TYPE_NORMAL);
                storageMetaInterface =
                        (new ValueMeta(measureConst + i, ValueMetaInterface.TYPE_NUMBER,
                                ValueMetaInterface.STORAGE_TYPE_NORMAL));
                valueMetaInterface.setStorageMetadata(storageMetaInterface);

            } else {
                valueMetaInterface = new ValueMeta(measureConst + i, ValueMetaInterface.TYPE_BINARY,
                        ValueMetaInterface.STORAGE_TYPE_BINARY_STRING);
                storageMetaInterface =
                        (new ValueMeta(measureConst + i, ValueMetaInterface.TYPE_STRING,
                                ValueMetaInterface.STORAGE_TYPE_NORMAL));
                valueMetaInterface.setStringEncoding(BYTE_ENCODING);
                valueMetaInterface.setStorageMetadata(storageMetaInterface);
                valueMetaInterface.getStorageMetadata().setStringEncoding(BYTE_ENCODING);
                valueMetaInterface.setStorageMetadata(storageMetaInterface);
            }
            out[l++] = valueMetaInterface;
        }
        valueMetaInterface = new ValueMeta("id", ValueMetaInterface.TYPE_BINARY,
                ValueMetaInterface.STORAGE_TYPE_BINARY_STRING);
        valueMetaInterface.setStorageMetadata((new ValueMeta("id", ValueMetaInterface.TYPE_STRING,
                ValueMetaInterface.STORAGE_TYPE_NORMAL)));
        valueMetaInterface.setStringEncoding(BYTE_ENCODING);
        valueMetaInterface.getStorageMetadata().setStringEncoding(BYTE_ENCODING);
        out[l++] = valueMetaInterface;
        data.outputRowMeta.setValueMetaList(Arrays.asList(out));
    }

    /**
     * Initialize and do work where other steps need to wait for...
     *
     * @param smi The metadata to work with
     * @param sdi The data to initialize
     * @return step initialize or not
     */
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (CarbonFactReaderMeta) smi;
        data = (CarbonFactReaderData) sdi;
        return super.init(smi, sdi);
    }

    /**
     * Dispose of this step: close files, empty logs, etc.
     *
     * @param smi The metadata to work with
     * @param sdi The data to dispose of
     */
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (CarbonFactReaderMeta) smi;
        data = (CarbonFactReaderData) sdi;
        super.dispose(smi, sdi);
    }

    /**
     * Thread class to which will be used to read the fact data
     */
    private final class DataProcessor implements Callable<Void> {
        public Void call() throws Exception {
            try {
                Object[] next = null;
                synchronized (LOCK) {
                    InMemoryTableStore inMemoryCubeInstance = InMemoryTableStore.getInstance();
                    String cubeUniqueName = meta.getSchemaName() + "_" + meta.getCubeName();
                    Cube cube = CarbonMetadata.getInstance().getCube(cubeUniqueName);
                    if (null == cube) {
                        LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                                "cube not loaded: " + meta.getTableName());
                        return null;
                    }
                    List<InMemoryTable> activeSlices =
                            inMemoryCubeInstance.getActiveSlices(cubeUniqueName);
                    List<InMemoryTable> querySlices = InMemoryLoadTableUtil
                            .getQuerySlices(activeSlices, meta.getLoadNameAndModificationTimeMap());
                    List<SliceExecutionInfo> infos = new ArrayList<SliceExecutionInfo>(
                            CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
                    String loadName = getLoadName();
                    dimAggInfo = new ArrayList<DimensionAggregatorInfo>(
                            CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
                    initQueryDims(cube);
                    initQueryMsrs(cube);
                    aggTypes = getAggTypes();
                    SliceMetaData sliceMataData = null;
                    SliceExecutionInfo latestSliceInfo = null;
                    InMemoryTable requiredSlice = null;
                    // for each slice we need create a slice info which will be
                    // used to
                    // execute the query
                    int currentSliceIndex = -1;
                    for (InMemoryTable slice : querySlices) {
                        // get the slice metadata for each slice
                        currentSliceIndex++;
                        if (null != slice.getDataCache(cube.getFactTableName())) {
                            sliceMataData =
                                    slice.getRsStore().getSliceMetaCache(cube.getFactTableName());
                        } else {
                            continue;
                        }
                        if (loadName.equals(slice.getLoadName())) {
                            requiredSlice = slice;
                            break;
                        }
                    }
                    if (null == requiredSlice) {
                        LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                                "No sctive slices for the request load folder : " + loadName);
                        return null;
                    }
                    latestSliceInfo =
                            getSliceExecutionInfo(cube, activeSlices, requiredSlice, sliceMataData,
                                    currentSliceIndex);
                    if (null == latestSliceInfo) {
                        LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                                "No slice execution info object created: " + meta.getTableName());
                        return null;
                    }
                    infos.add(latestSliceInfo);
                    SliceExecuter executor = new ColumnarParallelSliceExecutor();
                    CarbonIterator<QueryResult> resultIterator = executor.executeSlices(infos, null);
                    QueryResult queryResult = resultIterator.next();
                    QueryResultIterator queryResultIterator = queryResult.iterator();
                    ByteArrayWrapper key = null;
                    MeasureAggregator[] value = null;
                    int count = 0;
                    int j = 0;
                    while (queryResultIterator.hasNext()) {
                        key = queryResultIterator.getKey();
                        value = queryResultIterator.getValue();
                        count++;
                        // length incremented by 2 (1 for mdkey and 1 for count)
                        next = new Object[4];
                        next[j++] = value;
                        next[j++] = Double.valueOf(count);
                        // converting high card dims into single byte buffer [].
                        byte[] NoDictionaryByteArr = null;

                        if (null != key.getNoDictionaryValKeyList()) {
                            NoDictionaryByteArr = RemoveDictionaryUtil
                                    .convertListByteArrToSingleArr(key.getNoDictionaryValKeyList());
                        }
                        next[j++] = NoDictionaryByteArr;
                        next[j] = key.getMaskedKey();
                        putRow(data.outputRowMeta, next);
                        writeCounter++;
                        if (writeCounter % logCounter == 0) {
                            LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                                    "Record Procerssed For table: " + meta.getTableName());
                            String logMessage = "Carbon Fact Reader Step: Read: " + 0 + ": Write: "
                                    + writeCounter;
                            LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                                    logMessage);
                        }
                        j = 0;
                    }
                }
            } catch (Exception e) {
                LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, e, "");
                threadStatusObserver.notifyFailed(e);
                throw e;
            }
            return null;
        }
    }

    /**
     * Observer class for thread execution
     * In case of any failure we need stop all the running thread
     */
    private class ThreadStatusObserver {
        /**
         * Below method will be called if any thread fails during execution
         *
         * @param exception
         * @throws Exception
         */
        public void notifyFailed(Throwable exception) throws Exception {
            executorService.shutdownNow();
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, exception);
            throw new Exception(exception);
        }
    }

}
