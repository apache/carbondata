/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.processing.store;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorageForInt;
import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorageForNoInvertedIndex;
import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorageForNoInvertedIndexForShort;
import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorageForShort;
import org.apache.carbondata.core.datastore.columnar.ColumnGroupModel;
import org.apache.carbondata.core.datastore.columnar.IndexStorage;
import org.apache.carbondata.core.datastore.compression.WriterCompressModel;
import org.apache.carbondata.core.datastore.dataholder.CarbonWriteDataHolder;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.keygenerator.columnar.ColumnarSplitter;
import org.apache.carbondata.core.keygenerator.columnar.impl.MultiDimKeyVarLengthEquiSplitGenerator;
import org.apache.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.NodeHolder;
import org.apache.carbondata.core.util.ValueCompressionUtil;
import org.apache.carbondata.processing.datatypes.GenericDataType;
import org.apache.carbondata.processing.mdkeygen.file.FileManager;
import org.apache.carbondata.processing.mdkeygen.file.IFileManagerComposite;
import org.apache.carbondata.processing.store.colgroup.ColGroupBlockStorage;
import org.apache.carbondata.processing.store.colgroup.ColGroupDataHolder;
import org.apache.carbondata.processing.store.colgroup.ColGroupMinMax;
import org.apache.carbondata.processing.store.colgroup.ColumnDataHolder;
import org.apache.carbondata.processing.store.colgroup.DataHolder;
import org.apache.carbondata.processing.store.writer.CarbonDataWriterVo;
import org.apache.carbondata.processing.store.writer.CarbonFactDataWriter;
import org.apache.carbondata.processing.store.writer.exception.CarbonDataWriterException;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;
import org.apache.carbondata.processing.util.NonDictionaryUtil;

import org.apache.spark.sql.types.Decimal;

/**
 * Fact data handler class to handle the fact data
 */
public class CarbonFactDataHandlerColumnar implements CarbonFactHandler {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonFactDataHandlerColumnar.class.getName());
  /**
   * data writer
   */
  private CarbonFactDataWriter dataWriter;
  /**
   * File manager
   */
  private IFileManagerComposite fileManager;
  /**
   * total number of entries in blocklet
   */
  private int entryCount;
  private Map<Integer, GenericDataType> complexIndexMap;
  /**
   * measure count
   */
  private int measureCount;
  /**
   * measure count
   */
  private int dimensionCount;
  /**
   * index of mdkey in incoming rows
   */
  private int mdKeyIndex;
  /**
   * blocklet size
   */
  private int blockletSize;
  /**
   * mdkeyLength
   */
  private int mdkeyLength;
  /**
   * storeLocation
   */
  private String storeLocation;
  /**
   * databaseName
   */
  private String databaseName;
  /**
   * tableName
   */
  private String tableName;
  /**
   * table block size in MB
   */
  private int tableBlockSize;
  /**
   * otherMeasureIndex
   */
  private int[] otherMeasureIndex;
  /**
   * customMeasureIndex
   */
  private int[] customMeasureIndex;
  /**
   * dimLens
   */
  private int[] dimLens;
  /**
   * keyGenerator
   */
  private ColumnarSplitter columnarSplitter;
  /**
   * keyBlockHolder
   */
  private CarbonKeyBlockHolder[] keyBlockHolder;
  private boolean[] aggKeyBlock;
  private boolean[] isNoDictionary;
  private boolean isAggKeyBlock;
  private long processedDataCount;
  /**
   * thread pool size to be used for block sort
   */
  private int thread_pool_size;
  private KeyGenerator[] complexKeyGenerator;
  /**
   * isDataWritingRequest
   */
  //    private boolean isDataWritingRequest;

  private ExecutorService producerExecutorService;
  private List<Future<Void>> producerExecutorServiceTaskList;
  private ExecutorService consumerExecutorService;
  private List<Future<Void>> consumerExecutorServiceTaskList;
  private List<Object[]> dataRows;
  private int noDictionaryCount;
  private ColumnGroupModel colGrpModel;
  private int[] primitiveDimLens;
  private char[] type;
  private int[] completeDimLens;
  private boolean[] isUseInvertedIndex;
  /**
   * data file attributes which will used for file construction
   */
  private CarbonDataFileAttributes carbonDataFileAttributes;
  /**
   * semaphore which will used for managing node holder objects
   */
  private Semaphore semaphore;
  /**
   * counter that incremented for every job submitted to data writer thread
   */
  private int writerTaskSequenceCounter;
  /**
   * a private class that will hold the data for blocklets
   */
  private BlockletDataHolder blockletDataHolder;
  /**
   * a private class which will take each blocklet in order and write to a file
   */
  private Consumer consumer;
  /**
   * number of cores configured
   */
  private int numberOfCores;
  /**
   * integer that will be incremented for every new blocklet submitted to producer for processing
   * the data and decremented every time consumer fetches the blocklet for writing
   */
  private AtomicInteger blockletProcessingCount;
  /**
   * flag to check whether all blocklets have been finished writing
   */
  private boolean processingComplete;
  /**
   * data directory location in carbon store path
   */
  private String carbonDataDirectoryPath;
  /**
   * no of complex dimensions
   */
  private int complexColCount;

  /**
   * no of column blocks
   */
  private int columnStoreCount;

  /**
   * column schema present in the table
   */
  private List<ColumnSchema> wrapperColumnSchemaList;

  /**
   * boolean to check whether dimension
   * is of dictionary type or no dictionary time
   */
  private boolean[] dimensionType;

  /**
   * colCardinality for the merge case.
   */
  private int[] colCardinality;

  /**
   * Segment properties
   */
  private SegmentProperties segmentProperties;
  /**
   * flag to check for compaction flow
   */
  private boolean compactionFlow;

  private boolean useKettle;

  private int bucketNumber;

  private long schemaUpdatedTimeStamp;

  private String segmentId;

  private int taskExtension;

  /**
   * current data format version
   */
  private ColumnarFormatVersion version;

  /**
   * CarbonFactDataHandler constructor
   */
  public CarbonFactDataHandlerColumnar(CarbonFactDataHandlerModel carbonFactDataHandlerModel) {
    initParameters(carbonFactDataHandlerModel);
    this.dimensionCount = carbonFactDataHandlerModel.getDimensionCount();
    this.complexIndexMap = carbonFactDataHandlerModel.getComplexIndexMap();
    this.primitiveDimLens = carbonFactDataHandlerModel.getPrimitiveDimLens();
    this.useKettle = carbonFactDataHandlerModel.isUseKettle();
    this.isAggKeyBlock = Boolean.parseBoolean(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.AGGREAGATE_COLUMNAR_KEY_BLOCK,
            CarbonCommonConstants.AGGREAGATE_COLUMNAR_KEY_BLOCK_DEFAULTVALUE));
    this.carbonDataDirectoryPath = carbonFactDataHandlerModel.getCarbonDataDirectoryPath();
    this.complexColCount = getComplexColsCount();
    this.columnStoreCount =
        this.colGrpModel.getNoOfColumnStore() + noDictionaryCount + complexColCount;

    this.aggKeyBlock = new boolean[columnStoreCount];
    this.isNoDictionary = new boolean[columnStoreCount];
    this.bucketNumber = carbonFactDataHandlerModel.getBucketId();
    this.taskExtension = carbonFactDataHandlerModel.getTaskExtension();
    this.isUseInvertedIndex = new boolean[columnStoreCount];
    if (null != carbonFactDataHandlerModel.getIsUseInvertedIndex()) {
      for (int i = 0; i < isUseInvertedIndex.length; i++) {
        if (i < carbonFactDataHandlerModel.getIsUseInvertedIndex().length) {
          isUseInvertedIndex[i] = carbonFactDataHandlerModel.getIsUseInvertedIndex()[i];
        } else {
          isUseInvertedIndex[i] = true;
        }
      }
    }
    int noDictStartIndex = this.colGrpModel.getNoOfColumnStore();
    // setting true value for dims of high card
    for (int i = 0; i < noDictionaryCount; i++) {
      this.isNoDictionary[noDictStartIndex + i] = true;
    }

    if (isAggKeyBlock) {
      int noDictionaryValue = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.HIGH_CARDINALITY_VALUE,
              CarbonCommonConstants.HIGH_CARDINALITY_VALUE_DEFAULTVALUE));
      int[] columnSplits = colGrpModel.getColumnSplit();
      int dimCardinalityIndex = 0;
      int aggIndex = 0;
      for (int i = 0; i < columnSplits.length; i++) {
        if (colGrpModel.isColumnar(i) && dimLens[dimCardinalityIndex] < noDictionaryValue) {
          this.aggKeyBlock[aggIndex++] = true;
          continue;
        }
        dimCardinalityIndex += columnSplits[i];
        aggIndex++;
      }

      if (dimensionCount < dimLens.length) {
        int allColsCount = getColsCount(dimensionCount);
        List<Boolean> aggKeyBlockWithComplex = new ArrayList<Boolean>(allColsCount);
        for (int i = 0; i < dimensionCount; i++) {
          GenericDataType complexDataType = complexIndexMap.get(i);
          if (complexDataType != null) {
            complexDataType.fillAggKeyBlock(aggKeyBlockWithComplex, this.aggKeyBlock);
          } else {
            aggKeyBlockWithComplex.add(this.aggKeyBlock[i]);
          }
        }
        this.aggKeyBlock = new boolean[allColsCount];
        for (int i = 0; i < allColsCount; i++) {
          this.aggKeyBlock[i] = aggKeyBlockWithComplex.get(i);
        }
      }
      aggKeyBlock = arrangeUniqueBlockType(aggKeyBlock);
    }
    version = CarbonProperties.getInstance().getFormatVersion();
  }

  private void initParameters(CarbonFactDataHandlerModel carbonFactDataHandlerModel) {
    this.databaseName = carbonFactDataHandlerModel.getDatabaseName();
    this.tableBlockSize = carbonFactDataHandlerModel.getBlockSizeInMB();
    this.tableName = carbonFactDataHandlerModel.getTableName();
    this.type = carbonFactDataHandlerModel.getAggType();
    this.segmentProperties = carbonFactDataHandlerModel.getSegmentProperties();
    this.wrapperColumnSchemaList = carbonFactDataHandlerModel.getWrapperColumnSchema();
    this.colCardinality = carbonFactDataHandlerModel.getColCardinality();
    this.storeLocation = carbonFactDataHandlerModel.getStoreLocation();
    this.measureCount = carbonFactDataHandlerModel.getMeasureCount();
    this.mdkeyLength = carbonFactDataHandlerModel.getMdKeyLength();
    this.mdKeyIndex = carbonFactDataHandlerModel.getMdKeyIndex();
    this.noDictionaryCount = carbonFactDataHandlerModel.getNoDictionaryCount();
    this.colGrpModel = segmentProperties.getColumnGroupModel();
    this.completeDimLens = carbonFactDataHandlerModel.getDimLens();
    this.dimLens = this.segmentProperties.getDimColumnsCardinality();
    this.carbonDataFileAttributes = carbonFactDataHandlerModel.getCarbonDataFileAttributes();
    this.schemaUpdatedTimeStamp = carbonFactDataHandlerModel.getSchemaUpdatedTimeStamp();
    this.segmentId = carbonFactDataHandlerModel.getSegmentId();
    //TODO need to pass carbon table identifier to metadata
    CarbonTable carbonTable = CarbonMetadata.getInstance()
        .getCarbonTable(databaseName + CarbonCommonConstants.UNDERSCORE + tableName);
    dimensionType =
        CarbonUtil.identifyDimensionType(carbonTable.getDimensionByTableName(tableName));

    this.compactionFlow = carbonFactDataHandlerModel.isCompactionFlow();
    // in compaction flow the measure with decimal type will come as spark decimal.
    // need to convert it to byte array.
    if (compactionFlow) {
      try {
        numberOfCores = Integer.parseInt(CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.NUM_CORES_COMPACTING,
                CarbonCommonConstants.NUM_CORES_DEFAULT_VAL));
      } catch (NumberFormatException exc) {
        LOGGER.error("Configured value for property " + CarbonCommonConstants.NUM_CORES_COMPACTING
            + "is wrong.Falling back to the default value "
            + CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
        numberOfCores = Integer.parseInt(CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
      }
    } else {
      try {
        numberOfCores = Integer.parseInt(CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.NUM_CORES_LOADING,
                CarbonCommonConstants.NUM_CORES_DEFAULT_VAL));
      } catch (NumberFormatException exc) {
        LOGGER.error("Configured value for property " + CarbonCommonConstants.NUM_CORES_LOADING
            + "is wrong.Falling back to the default value "
            + CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
        numberOfCores = Integer.parseInt(CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
      }
    }

    blockletProcessingCount = new AtomicInteger(0);
    producerExecutorService = Executors.newFixedThreadPool(numberOfCores);
    producerExecutorServiceTaskList =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    LOGGER.info("Initializing writer executors");
    consumerExecutorService = Executors.newFixedThreadPool(1);
    consumerExecutorServiceTaskList = new ArrayList<>(1);
    semaphore = new Semaphore(numberOfCores);
    blockletDataHolder = new BlockletDataHolder();
    consumer = new Consumer(blockletDataHolder);
    consumerExecutorServiceTaskList.add(consumerExecutorService.submit(consumer));
  }

  private boolean[] arrangeUniqueBlockType(boolean[] aggKeyBlock) {
    int counter = 0;
    boolean[] uniqueBlock = new boolean[aggKeyBlock.length];
    for (int i = 0; i < dimensionType.length; i++) {
      if (dimensionType[i]) {
        uniqueBlock[i] = aggKeyBlock[counter++];
      } else {
        uniqueBlock[i] = false;
      }
    }
    return uniqueBlock;
  }

  private void setComplexMapSurrogateIndex(int dimensionCount) {
    int surrIndex = 0;
    for (int i = 0; i < dimensionCount; i++) {
      GenericDataType complexDataType = complexIndexMap.get(i);
      if (complexDataType != null) {
        List<GenericDataType> primitiveTypes = new ArrayList<GenericDataType>();
        complexDataType.getAllPrimitiveChildren(primitiveTypes);
        for (GenericDataType eachPrimitive : primitiveTypes) {
          eachPrimitive.setSurrogateIndex(surrIndex++);
        }
      } else {
        surrIndex++;
      }
    }
  }

  /**
   * This method will be used to get and update the step properties which will
   * required to run this step
   *
   * @throws CarbonDataWriterException
   */
  public void initialise() throws CarbonDataWriterException {
    fileManager = new FileManager();
    fileManager.setName(new File(this.storeLocation).getName());
    setWritingConfiguration();
  }

  /**
   * below method will be used to add row to store
   *
   * @param row
   * @throws CarbonDataWriterException
   */
  public void addDataToStore(Object[] row) throws CarbonDataWriterException {
    dataRows.add(row);
    this.entryCount++;
    // if entry count reaches to leaf node size then we are ready to
    // write
    // this to leaf node file and update the intermediate files
    if (this.entryCount == this.blockletSize) {
      try {
        semaphore.acquire();
        producerExecutorServiceTaskList.add(producerExecutorService
            .submit(new Producer(blockletDataHolder, dataRows, ++writerTaskSequenceCounter)));
        blockletProcessingCount.incrementAndGet();
        // set the entry count to zero
        processedDataCount += entryCount;
        LOGGER.info("Total Number Of records added to store: " + processedDataCount);
        dataRows = new ArrayList<>(this.blockletSize);
        this.entryCount = 0;
      } catch (InterruptedException e) {
        LOGGER.error(e, e.getMessage());
        throw new CarbonDataWriterException(e.getMessage(), e);
      }
    }
  }

  // TODO remove after kettle flow is removed
  private NodeHolder processDataRows(List<Object[]> dataRows) throws CarbonDataWriterException {
    Object[] max = new Object[measureCount];
    Object[] min = new Object[measureCount];
    int[] decimal = new int[measureCount];
    Object[] uniqueValue = new Object[measureCount];
    // to store index of the measure columns which are null
    BitSet[] nullValueIndexBitSet = getMeasureNullValueIndexBitSet(measureCount);
    for (int i = 0; i < max.length; i++) {
      if (type[i] == CarbonCommonConstants.BIG_INT_MEASURE) {
        max[i] = Long.MIN_VALUE;
      } else if (type[i] == CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE) {
        max[i] = -Double.MAX_VALUE;
      } else if (type[i] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
        max[i] = new BigDecimal(-Double.MAX_VALUE);
      } else {
        max[i] = 0.0;
      }
    }
    for (int i = 0; i < min.length; i++) {
      if (type[i] == CarbonCommonConstants.BIG_INT_MEASURE) {
        min[i] = Long.MAX_VALUE;
        uniqueValue[i] = Long.MIN_VALUE;
      } else if (type[i] == CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE) {
        min[i] = Double.MAX_VALUE;
        uniqueValue[i] = Double.MIN_VALUE;
      } else if (type[i] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
        min[i] = new BigDecimal(Double.MAX_VALUE);
        uniqueValue[i] = new BigDecimal(Double.MIN_VALUE);
      } else {
        min[i] = 0.0;
        uniqueValue[i] = 0.0;
      }
    }
    for (int i = 0; i < decimal.length; i++) {
      decimal[i] = 0;
    }

    byte[] startKey = null;
    byte[] endKey = null;
    byte[] noDictStartKey = null;
    byte[] noDictEndKey = null;
    CarbonWriteDataHolder[] dataHolder = initialiseDataHolder(dataRows.size());
    CarbonWriteDataHolder keyDataHolder = initialiseKeyBlockHolder(dataRows.size());
    CarbonWriteDataHolder noDictionaryKeyDataHolder = null;
    if ((noDictionaryCount + complexColCount) > 0) {
      noDictionaryKeyDataHolder = initialiseKeyBlockHolder(dataRows.size());
    }

    for (int count = 0; count < dataRows.size(); count++) {
      Object[] row = dataRows.get(count);
      byte[] mdKey = (byte[]) row[this.mdKeyIndex];
      byte[] noDictionaryKey = null;
      if (noDictionaryCount > 0 || complexIndexMap.size() > 0) {
        noDictionaryKey = (byte[]) row[this.mdKeyIndex - 1];
      }
      ByteBuffer byteBuffer = null;
      byte[] b = null;
      if (count == 0) {
        startKey = mdKey;
        noDictStartKey = noDictionaryKey;
      }
      endKey = mdKey;
      noDictEndKey = noDictionaryKey;
      // add to key store
      if (mdKey.length > 0) {
        keyDataHolder.setWritableByteArrayValueByIndex(count, mdKey);
      }
      // for storing the byte [] for high card.
      if (noDictionaryCount > 0 || complexIndexMap.size() > 0) {
        noDictionaryKeyDataHolder.setWritableByteArrayValueByIndex(count, noDictionaryKey);
      }
      //Add all columns to keyDataHolder
      keyDataHolder.setWritableByteArrayValueByIndex(count, this.mdKeyIndex, row);
      // CHECKSTYLE:OFF Approval No:Approval-351
      for (int k = 0; k < otherMeasureIndex.length; k++) {
        if (type[otherMeasureIndex[k]] == CarbonCommonConstants.BIG_INT_MEASURE) {
          if (null == row[otherMeasureIndex[k]]) {
            nullValueIndexBitSet[otherMeasureIndex[k]].set(count);
            dataHolder[otherMeasureIndex[k]].setWritableLongValueByIndex(count, 0L);
          } else {
            dataHolder[otherMeasureIndex[k]]
                .setWritableLongValueByIndex(count, row[otherMeasureIndex[k]]);
          }
        } else {
          if (null == row[otherMeasureIndex[k]]) {
            nullValueIndexBitSet[otherMeasureIndex[k]].set(count);
            dataHolder[otherMeasureIndex[k]].setWritableDoubleValueByIndex(count, 0.0);
          } else {
            dataHolder[otherMeasureIndex[k]]
                .setWritableDoubleValueByIndex(count, row[otherMeasureIndex[k]]);
          }
        }
      }
      calculateMaxMin(max, min, decimal, otherMeasureIndex, row);
      for (int i = 0; i < customMeasureIndex.length; i++) {
        if (null == row[customMeasureIndex[i]]
            && type[customMeasureIndex[i]] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
          BigDecimal val = BigDecimal.valueOf(0);
          b = DataTypeUtil.bigDecimalToByte(val);
          nullValueIndexBitSet[customMeasureIndex[i]].set(count);
        } else {
          if (this.compactionFlow) {
            BigDecimal bigDecimal = ((Decimal) row[customMeasureIndex[i]]).toJavaBigDecimal();
            b = DataTypeUtil.bigDecimalToByte(bigDecimal);
          } else {
            b = (byte[]) row[customMeasureIndex[i]];
          }
        }
        byteBuffer = ByteBuffer.allocate(b.length + CarbonCommonConstants.INT_SIZE_IN_BYTE);
        byteBuffer.putInt(b.length);
        byteBuffer.put(b);
        byteBuffer.flip();
        b = byteBuffer.array();
        dataHolder[customMeasureIndex[i]].setWritableByteArrayValueByIndex(count, b);
      }
      calculateMaxMin(max, min, decimal, customMeasureIndex, row);
    }
    calculateUniqueValue(min, uniqueValue);
    byte[][] byteArrayValues = keyDataHolder.getByteArrayValues().clone();
    byte[][] noDictionaryValueHolder = null;
    if ((noDictionaryCount + complexColCount) > 0) {
      noDictionaryValueHolder = noDictionaryKeyDataHolder.getByteArrayValues();
    }
    WriterCompressModel compressionModel = ValueCompressionUtil
        .getWriterCompressModel(max, min, decimal, uniqueValue, type, new byte[max.length]);
    byte[][] writableMeasureDataArray =
        StoreFactory.createDataStore(compressionModel).getWritableMeasureDataArray(dataHolder)
            .clone();
    NodeHolder nodeHolder =
        getNodeHolderObject(writableMeasureDataArray, byteArrayValues, dataRows.size(), startKey,
            endKey, compressionModel, noDictionaryValueHolder, noDictStartKey, noDictEndKey,
            nullValueIndexBitSet);
    LOGGER.info("Number Of records processed: " + dataRows.size());
    return nodeHolder;
  }

  private NodeHolder processDataRowsWithOutKettle(List<Object[]> dataRows)
      throws CarbonDataWriterException {
    Object[] max = new Object[measureCount];
    Object[] min = new Object[measureCount];
    int[] decimal = new int[measureCount];
    Object[] uniqueValue = new Object[measureCount];
    // to store index of the measure columns which are null
    BitSet[] nullValueIndexBitSet = getMeasureNullValueIndexBitSet(measureCount);
    for (int i = 0; i < max.length; i++) {
      if (type[i] == CarbonCommonConstants.BIG_INT_MEASURE) {
        max[i] = Long.MIN_VALUE;
      } else if (type[i] == CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE) {
        max[i] = -Double.MAX_VALUE;
      } else if (type[i] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
        max[i] = new BigDecimal(0.0);
      } else {
        max[i] = 0.0;
      }
    }
    for (int i = 0; i < min.length; i++) {
      if (type[i] == CarbonCommonConstants.BIG_INT_MEASURE) {
        min[i] = Long.MAX_VALUE;
        uniqueValue[i] = Long.MIN_VALUE;
      } else if (type[i] == CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE) {
        min[i] = Double.MAX_VALUE;
        uniqueValue[i] = Double.MIN_VALUE;
      } else if (type[i] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
        min[i] = new BigDecimal(Double.MAX_VALUE);
        uniqueValue[i] = new BigDecimal(Double.MIN_VALUE);
      } else {
        min[i] = 0.0;
        uniqueValue[i] = 0.0;
      }
    }
    for (int i = 0; i < decimal.length; i++) {
      decimal[i] = 0;
    }

    byte[] startKey = null;
    byte[] endKey = null;
    byte[][] noDictStartKey = null;
    byte[][] noDictEndKey = null;
    CarbonWriteDataHolder[] dataHolder = initialiseDataHolder(dataRows.size());
    CarbonWriteDataHolder keyDataHolder = initialiseKeyBlockHolderWithOutKettle(dataRows.size());
    CarbonWriteDataHolder noDictionaryKeyDataHolder = null;
    if ((noDictionaryCount + complexColCount) > 0) {
      noDictionaryKeyDataHolder = initialiseKeyBlockHolderForNonDictionary(dataRows.size());
    }

    for (int count = 0; count < dataRows.size(); count++) {
      Object[] row = dataRows.get(count);
      byte[] mdKey = (byte[]) row[this.mdKeyIndex];
      byte[][] noDictionaryKey = null;
      if (noDictionaryCount > 0 || complexIndexMap.size() > 0) {
        noDictionaryKey = (byte[][]) row[this.mdKeyIndex - 1];
      }
      ByteBuffer byteBuffer = null;
      byte[] b = null;
      if (count == 0) {
        startKey = mdKey;
        noDictStartKey = noDictionaryKey;
      }
      endKey = mdKey;
      noDictEndKey = noDictionaryKey;
      // add to key store
      if (mdKey.length > 0) {
        keyDataHolder.setWritableByteArrayValueByIndex(count, mdKey);
      }
      // for storing the byte [] for high card.
      if (noDictionaryCount > 0 || complexIndexMap.size() > 0) {
        noDictionaryKeyDataHolder.setWritableNonDictByteArrayValueByIndex(count, noDictionaryKey);
      }

      for (int k = 0; k < otherMeasureIndex.length; k++) {
        if (type[otherMeasureIndex[k]] == CarbonCommonConstants.BIG_INT_MEASURE) {
          if (null == row[otherMeasureIndex[k]]) {
            nullValueIndexBitSet[otherMeasureIndex[k]].set(count);
            dataHolder[otherMeasureIndex[k]].setWritableLongValueByIndex(count, 0L);
          } else {
            dataHolder[otherMeasureIndex[k]]
                .setWritableLongValueByIndex(count, row[otherMeasureIndex[k]]);
          }
        } else {
          if (null == row[otherMeasureIndex[k]]) {
            nullValueIndexBitSet[otherMeasureIndex[k]].set(count);
            dataHolder[otherMeasureIndex[k]].setWritableDoubleValueByIndex(count, 0.0);
          } else {
            dataHolder[otherMeasureIndex[k]]
                .setWritableDoubleValueByIndex(count, row[otherMeasureIndex[k]]);
          }
        }
      }
      calculateMaxMin(max, min, decimal, otherMeasureIndex, row);
      for (int i = 0; i < customMeasureIndex.length; i++) {
        if (null == row[customMeasureIndex[i]]
            && type[customMeasureIndex[i]] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
          BigDecimal val = BigDecimal.valueOf(0);
          b = DataTypeUtil.bigDecimalToByte(val);
          nullValueIndexBitSet[customMeasureIndex[i]].set(count);
        } else {
          if (this.compactionFlow) {
            BigDecimal bigDecimal = ((Decimal) row[customMeasureIndex[i]]).toJavaBigDecimal();
            b = DataTypeUtil.bigDecimalToByte(bigDecimal);
          } else {
            b = (byte[]) row[customMeasureIndex[i]];
          }
        }
        BigDecimal value = DataTypeUtil.byteToBigDecimal(b);
        String[] bigdVals = value.toPlainString().split("\\.");
        long[] bigDvalue = new long[2];
        if (bigdVals.length == 2) {
          bigDvalue[0] = Long.parseLong(bigdVals[0]);
          BigDecimal bd = new BigDecimal(CarbonCommonConstants.POINT + bigdVals[1]);
          bigDvalue[1] = (long) (bd.doubleValue() * Math.pow(10, value.scale()));
        } else {
          bigDvalue[0] = Long.parseLong(bigdVals[0]);
        }
        byteBuffer = ByteBuffer.allocate(b.length + CarbonCommonConstants.INT_SIZE_IN_BYTE);
        byteBuffer.putInt(b.length);
        byteBuffer.put(b);
        byteBuffer.flip();
        b = byteBuffer.array();
        dataHolder[customMeasureIndex[i]].setWritableByteArrayValueByIndex(count, b);
      }
      calculateMaxMin(max, min, decimal, customMeasureIndex, row);
    }
    calculateUniqueValue(min, uniqueValue);
    byte[][] byteArrayValues = keyDataHolder.getByteArrayValues().clone();
    byte[][][] noDictionaryValueHolder = null;
    if ((noDictionaryCount + complexColCount) > 0) {
      noDictionaryValueHolder = noDictionaryKeyDataHolder.getNonDictByteArrayValues();
    }
    WriterCompressModel compressionModel = ValueCompressionUtil
        .getWriterCompressModel(max, min, decimal, uniqueValue, type, new byte[max.length]);
    byte[][] writableMeasureDataArray =
        StoreFactory.createDataStore(compressionModel).getWritableMeasureDataArray(dataHolder)
            .clone();
    NodeHolder nodeHolder =
        getNodeHolderObjectWithOutKettle(writableMeasureDataArray, byteArrayValues, dataRows.size(),
            startKey, endKey, compressionModel, noDictionaryValueHolder, noDictStartKey,
            noDictEndKey, nullValueIndexBitSet);
    LOGGER.info("Number Of records processed: " + dataRows.size());
    return nodeHolder;
  }

  // TODO remove after kettle flow is removed
  private NodeHolder getNodeHolderObject(byte[][] dataHolderLocal, byte[][] byteArrayValues,
      int entryCountLocal, byte[] startkeyLocal, byte[] endKeyLocal,
      WriterCompressModel compressionModel, byte[][] noDictionaryData, byte[] noDictionaryStartKey,
      byte[] noDictionaryEndKey, BitSet[] nullValueIndexBitSet) throws CarbonDataWriterException {
    byte[][][] noDictionaryColumnsData = null;
    List<ArrayList<byte[]>> colsAndValues = new ArrayList<ArrayList<byte[]>>();
    int complexColCount = getComplexColsCount();

    for (int i = 0; i < complexColCount; i++) {
      colsAndValues.add(new ArrayList<byte[]>());
    }
    int noOfColumn = colGrpModel.getNoOfColumnStore();
    DataHolder[] dataHolders = getDataHolders(noOfColumn, byteArrayValues.length);
    for (int i = 0; i < byteArrayValues.length; i++) {
      byte[][] splitKey = columnarSplitter.splitKey(byteArrayValues[i]);

      for (int j = 0; j < splitKey.length; j++) {
        dataHolders[j].addData(splitKey[j], i);
      }
    }
    if (noDictionaryCount > 0 || complexIndexMap.size() > 0) {
      noDictionaryColumnsData = new byte[noDictionaryCount][noDictionaryData.length][];
      for (int i = 0; i < noDictionaryData.length; i++) {
        int complexColumnIndex = primitiveDimLens.length + noDictionaryCount;
        byte[][] splitKey = NonDictionaryUtil
            .splitNoDictionaryKey(noDictionaryData[i], noDictionaryCount + complexIndexMap.size());

        int complexTypeIndex = 0;
        for (int j = 0; j < splitKey.length; j++) {
          //nodictionary Columns
          if (j < noDictionaryCount) {
            noDictionaryColumnsData[j][i] = splitKey[j];
          }
          //complex types
          else {
            // Need to write columnar block from complex byte array
            int index = complexColumnIndex - noDictionaryCount;
            GenericDataType complexDataType = complexIndexMap.get(index);
            complexColumnIndex++;
            if (complexDataType != null) {
              List<ArrayList<byte[]>> columnsArray = new ArrayList<ArrayList<byte[]>>();
              for (int k = 0; k < complexDataType.getColsCount(); k++) {
                columnsArray.add(new ArrayList<byte[]>());
              }

              try {
                ByteBuffer complexDataWithoutBitPacking = ByteBuffer.wrap(splitKey[j]);
                byte[] complexTypeData = new byte[complexDataWithoutBitPacking.getShort()];
                complexDataWithoutBitPacking.get(complexTypeData);

                ByteBuffer byteArrayInput = ByteBuffer.wrap(complexTypeData);
                ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
                DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutput);
                complexDataType
                    .parseAndBitPack(byteArrayInput, dataOutputStream, this.complexKeyGenerator);
                complexDataType.getColumnarDataForComplexType(columnsArray,
                    ByteBuffer.wrap(byteArrayOutput.toByteArray()));
                byteArrayOutput.close();
              } catch (IOException e) {
                throw new CarbonDataWriterException(
                    "Problem while bit packing and writing complex datatype", e);
              } catch (KeyGenException e) {
                throw new CarbonDataWriterException(
                    "Problem while bit packing and writing complex datatype", e);
              }

              for (ArrayList<byte[]> eachColumn : columnsArray) {
                colsAndValues.get(complexTypeIndex++).addAll(eachColumn);
              }
            } else {
              // This case not possible as ComplexType is the last columns
            }
          }
        }
      }
    }
    thread_pool_size = Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.NUM_CORES_BLOCK_SORT,
            CarbonCommonConstants.NUM_CORES_BLOCK_SORT_DEFAULT_VAL));
    ExecutorService executorService = Executors.newFixedThreadPool(thread_pool_size);
    List<Future<IndexStorage>> submit = new ArrayList<Future<IndexStorage>>(
        primitiveDimLens.length + noDictionaryCount + complexColCount);
    int i = 0;
    int dictionaryColumnCount = -1;
    int noDictionaryColumnCount = -1;
    for (i = 0; i < dimensionType.length; i++) {
      if (dimensionType[i]) {
        dictionaryColumnCount++;
        if (colGrpModel.isColumnar(dictionaryColumnCount)) {
          submit.add(executorService.submit(
              new BlockSortThread(i, dataHolders[dictionaryColumnCount].getData(), true,
                  isUseInvertedIndex[i])));
        } else {
          submit.add(
              executorService.submit(new ColGroupBlockStorage(dataHolders[dictionaryColumnCount])));
        }
      } else {
        submit.add(executorService.submit(
            new BlockSortThread(i, noDictionaryColumnsData[++noDictionaryColumnCount], false, true,
                true, isUseInvertedIndex[i])));
      }
    }
    for (int k = 0; k < complexColCount; k++) {
      submit.add(executorService.submit(new BlockSortThread(i++,
          colsAndValues.get(k).toArray(new byte[colsAndValues.get(k).size()][]), false, true)));
    }
    executorService.shutdown();
    try {
      executorService.awaitTermination(1, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      LOGGER.error(e, e.getMessage());
    }
    IndexStorage[] blockStorage =
        new IndexStorage[colGrpModel.getNoOfColumnStore() + noDictionaryCount + complexColCount];
    try {
      for (int k = 0; k < blockStorage.length; k++) {
        blockStorage[k] = submit.get(k).get();
      }
    } catch (Exception e) {
      LOGGER.error(e, e.getMessage());
    }
    return this.dataWriter
        .buildDataNodeHolder(blockStorage, dataHolderLocal, entryCountLocal, startkeyLocal,
            endKeyLocal, compressionModel, noDictionaryStartKey, noDictionaryEndKey,
            nullValueIndexBitSet);
  }

  private NodeHolder getNodeHolderObjectWithOutKettle(byte[][] dataHolderLocal,
      byte[][] byteArrayValues, int entryCountLocal, byte[] startkeyLocal, byte[] endKeyLocal,
      WriterCompressModel compressionModel, byte[][][] noDictionaryData,
      byte[][] noDictionaryStartKey, byte[][] noDictionaryEndKey, BitSet[] nullValueIndexBitSet)
      throws CarbonDataWriterException {
    byte[][][] noDictionaryColumnsData = null;
    List<ArrayList<byte[]>> colsAndValues = new ArrayList<ArrayList<byte[]>>();
    int complexColCount = getComplexColsCount();

    for (int i = 0; i < complexColCount; i++) {
      colsAndValues.add(new ArrayList<byte[]>());
    }
    int noOfColumn = colGrpModel.getNoOfColumnStore();
    DataHolder[] dataHolders = getDataHolders(noOfColumn, byteArrayValues.length);
    for (int i = 0; i < byteArrayValues.length; i++) {
      byte[][] splitKey = columnarSplitter.splitKey(byteArrayValues[i]);

      for (int j = 0; j < splitKey.length; j++) {
        dataHolders[j].addData(splitKey[j], i);
      }
    }
    if (noDictionaryCount > 0 || complexIndexMap.size() > 0) {
      noDictionaryColumnsData = new byte[noDictionaryCount][noDictionaryData.length][];
      for (int i = 0; i < noDictionaryData.length; i++) {
        int complexColumnIndex = primitiveDimLens.length + noDictionaryCount;
        byte[][] splitKey = noDictionaryData[i];

        int complexTypeIndex = 0;
        for (int j = 0; j < splitKey.length; j++) {
          //nodictionary Columns
          if (j < noDictionaryCount) {
            int keyLength = splitKey[j].length;
            byte[] newKey = new byte[keyLength + 2];
            ByteBuffer buffer = ByteBuffer.wrap(newKey);
            buffer.putShort((short) keyLength);
            System.arraycopy(splitKey[j], 0, newKey, 2, keyLength);
            noDictionaryColumnsData[j][i] = newKey;
          }
          //complex types
          else {
            // Need to write columnar block from complex byte array
            int index = complexColumnIndex - noDictionaryCount;
            GenericDataType complexDataType = complexIndexMap.get(index);
            complexColumnIndex++;
            if (complexDataType != null) {
              List<ArrayList<byte[]>> columnsArray = new ArrayList<ArrayList<byte[]>>();
              for (int k = 0; k < complexDataType.getColsCount(); k++) {
                columnsArray.add(new ArrayList<byte[]>());
              }

              try {
                ByteBuffer byteArrayInput = ByteBuffer.wrap(splitKey[j]);
                ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
                DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutput);
                complexDataType
                    .parseAndBitPack(byteArrayInput, dataOutputStream, this.complexKeyGenerator);
                complexDataType.getColumnarDataForComplexType(columnsArray,
                    ByteBuffer.wrap(byteArrayOutput.toByteArray()));
                byteArrayOutput.close();
              } catch (IOException e) {
                throw new CarbonDataWriterException(
                    "Problem while bit packing and writing complex datatype", e);
              } catch (KeyGenException e) {
                throw new CarbonDataWriterException(
                    "Problem while bit packing and writing complex datatype", e);
              }

              for (ArrayList<byte[]> eachColumn : columnsArray) {
                colsAndValues.get(complexTypeIndex++).addAll(eachColumn);
              }
            } else {
              // This case not possible as ComplexType is the last columns
            }
          }
        }
      }
    }
    thread_pool_size = Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.NUM_CORES_BLOCK_SORT,
            CarbonCommonConstants.NUM_CORES_BLOCK_SORT_DEFAULT_VAL));
    ExecutorService executorService = Executors.newFixedThreadPool(thread_pool_size);
    List<Future<IndexStorage>> submit = new ArrayList<Future<IndexStorage>>(
        primitiveDimLens.length + noDictionaryCount + complexColCount);
    int i = 0;
    int dictionaryColumnCount = -1;
    int noDictionaryColumnCount = -1;
    for (i = 0; i < dimensionType.length; i++) {
      if (dimensionType[i]) {
        dictionaryColumnCount++;
        if (colGrpModel.isColumnar(dictionaryColumnCount)) {
          submit.add(executorService.submit(
              new BlockSortThread(i, dataHolders[dictionaryColumnCount].getData(), true,
                  isUseInvertedIndex[i])));
        } else {
          submit.add(
              executorService.submit(new ColGroupBlockStorage(dataHolders[dictionaryColumnCount])));
        }
      } else {
        submit.add(executorService.submit(
            new BlockSortThread(i, noDictionaryColumnsData[++noDictionaryColumnCount], false, true,
                true, isUseInvertedIndex[i])));
      }
    }
    for (int k = 0; k < complexColCount; k++) {
      submit.add(executorService.submit(new BlockSortThread(i++,
          colsAndValues.get(k).toArray(new byte[colsAndValues.get(k).size()][]), false, true)));
    }
    executorService.shutdown();
    try {
      executorService.awaitTermination(1, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      LOGGER.error(e, e.getMessage());
    }
    IndexStorage[] blockStorage =
        new IndexStorage[colGrpModel.getNoOfColumnStore() + noDictionaryCount + complexColCount];
    try {
      for (int k = 0; k < blockStorage.length; k++) {
        blockStorage[k] = submit.get(k).get();
      }
    } catch (Exception e) {
      LOGGER.error(e, e.getMessage());
    }
    byte[] composedNonDictStartKey = null;
    byte[] composedNonDictEndKey = null;
    if (noDictionaryStartKey != null) {
      composedNonDictStartKey =
          NonDictionaryUtil.packByteBufferIntoSingleByteArray(noDictionaryStartKey);
      composedNonDictEndKey =
          NonDictionaryUtil.packByteBufferIntoSingleByteArray(noDictionaryEndKey);
    }
    return this.dataWriter
        .buildDataNodeHolder(blockStorage, dataHolderLocal, entryCountLocal, startkeyLocal,
            endKeyLocal, compressionModel, composedNonDictStartKey, composedNonDictEndKey,
            nullValueIndexBitSet);
  }

  /**
   * DataHolder will have all row mdkey data
   *
   * @param noOfColumn : no of column participated in mdkey
   * @param noOfRow    : total no of row
   * @return : dataholder
   */
  private DataHolder[] getDataHolders(int noOfColumn, int noOfRow) {
    DataHolder[] dataHolders = new DataHolder[noOfColumn];
    int colGrpId = -1;
    for (int colGrp = 0; colGrp < noOfColumn; colGrp++) {
      if (colGrpModel.isColumnar(colGrp)) {
        dataHolders[colGrp] = new ColumnDataHolder(noOfRow);
      } else {
        ColGroupMinMax colGrpMinMax = new ColGroupMinMax(segmentProperties, ++colGrpId);
        dataHolders[colGrp] =
            new ColGroupDataHolder(this.columnarSplitter.getBlockKeySize()[colGrp], noOfRow,
                colGrpMinMax);
      }
    }
    return dataHolders;
  }

  /**
   * below method will be used to finish the data handler
   *
   * @throws CarbonDataWriterException
   */
  public void finish() throws CarbonDataWriterException {
    // still some data is present in stores if entryCount is more
    // than 0
    if (this.entryCount > 0) {
      producerExecutorServiceTaskList.add(producerExecutorService
          .submit(new Producer(blockletDataHolder, dataRows, ++writerTaskSequenceCounter)));
      blockletProcessingCount.incrementAndGet();
      processedDataCount += entryCount;
    }
    closeWriterExecutionService(producerExecutorService);
    processWriteTaskSubmitList(producerExecutorServiceTaskList);
    processingComplete = true;
  }

  /**
   * This method will close writer execution service and get the node holders and
   * add them to node holder list
   *
   * @param service the service to shutdown
   * @throws CarbonDataWriterException
   */
  private void closeWriterExecutionService(ExecutorService service)
      throws CarbonDataWriterException {
    try {
      service.shutdown();
      service.awaitTermination(1, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      LOGGER.error(e, e.getMessage());
      throw new CarbonDataWriterException(e.getMessage());
    }
  }

  /**
   * This method will iterate through future task list and check if any exception
   * occurred during the thread execution
   *
   * @param taskList
   * @throws CarbonDataWriterException
   */
  private void processWriteTaskSubmitList(List<Future<Void>> taskList)
      throws CarbonDataWriterException {
    for (int i = 0; i < taskList.size(); i++) {
      try {
        taskList.get(i).get();
      } catch (InterruptedException e) {
        LOGGER.error(e, e.getMessage());
        throw new CarbonDataWriterException(e.getMessage(), e);
      } catch (ExecutionException e) {
        LOGGER.error(e, e.getMessage());
        throw new CarbonDataWriterException(e.getMessage(), e);
      }
    }
  }

  private int getColsCount(int columnSplit) {
    int count = 0;
    for (int i = 0; i < columnSplit; i++) {
      GenericDataType complexDataType = complexIndexMap.get(i);
      if (complexDataType != null) {
        count += complexDataType.getColsCount();
      } else count++;
    }
    return count;
  }

  private int getComplexColsCount() {
    int count = 0;
    for (int i = 0; i < dimensionCount; i++) {
      GenericDataType complexDataType = complexIndexMap.get(i);
      if (complexDataType != null) {
        count += complexDataType.getColsCount();
      }
    }
    return count;
  }

  /**
   * below method will be used to close the handler
   */
  public void closeHandler() throws CarbonDataWriterException {
    if (null != this.dataWriter) {
      // wait until all blocklets have been finished writing
      while (blockletProcessingCount.get() > 0) {
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          throw new CarbonDataWriterException(e.getMessage());
        }
      }
      consumerExecutorService.shutdownNow();
      processWriteTaskSubmitList(consumerExecutorServiceTaskList);
      this.dataWriter.writeBlockletInfoToFile();
      LOGGER.info("All blocklets have been finished writing");
      // close all the open stream for both the files
      this.dataWriter.closeWriter();
      // rename the bad record in progress to normal
      CarbonDataProcessorUtil.renameBadRecordsFromInProgressToNormal(
          this.databaseName + File.separator + this.tableName + File.separator + this.segmentId
              + File.separator + this.carbonDataFileAttributes.getTaskId());
    }
    this.dataWriter = null;
    this.keyBlockHolder = null;
  }

  /**
   * @param value
   * @return it return no of value after decimal
   */
  private int getDecimalCount(double value) {
    String strValue = BigDecimal.valueOf(Math.abs(value)).toPlainString();
    int integerPlaces = strValue.indexOf('.');
    int decimalPlaces = 0;
    if (-1 != integerPlaces) {
      decimalPlaces = strValue.length() - integerPlaces - 1;
    }
    return decimalPlaces;
  }

  /**
   * This method will be used to update the max value for each measure
   */
  private void calculateMaxMin(Object[] max, Object[] min, int[] decimal, int[] msrIndex,
      Object[] row) {
    // Update row level min max
    for (int i = 0; i < msrIndex.length; i++) {
      int count = msrIndex[i];
      if (row[count] != null) {
        if (type[count] == CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE) {
          double value = (double) row[count];
          double maxVal = (double) max[count];
          double minVal = (double) min[count];
          max[count] = (maxVal > value ? max[count] : value);
          min[count] = (minVal < value ? min[count] : value);
          int num = getDecimalCount(value);
          decimal[count] = (decimal[count] > num ? decimal[count] : num);
        } else if (type[count] == CarbonCommonConstants.BIG_INT_MEASURE) {
          long value = (long) row[count];
          long maxVal = (long) max[count];
          long minVal = (long) min[count];
          max[count] = (maxVal > value ? max[count] : value);
          min[count] = (minVal < value ? min[count] : value);
        } else if (type[count] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
          byte[] buff = null;
          // in compaction flow the measure with decimal type will come as spark decimal.
          // need to convert it to byte array.
          if (this.compactionFlow) {
            BigDecimal bigDecimal = ((Decimal) row[count]).toJavaBigDecimal();
            buff = DataTypeUtil.bigDecimalToByte(bigDecimal);
          } else {
            buff = (byte[]) row[count];
          }
          BigDecimal value = DataTypeUtil.byteToBigDecimal(buff);
          decimal[count] = value.scale();
        }
      }
    }
  }

  /**
   * This method will calculate the unique value which will be used as storage
   * key for null values of measures
   *
   * @param minValue
   * @param uniqueValue
   */
  private void calculateUniqueValue(Object[] minValue, Object[] uniqueValue) {
    for (int i = 0; i < measureCount; i++) {
      if (type[i] == CarbonCommonConstants.BIG_INT_MEASURE) {
        uniqueValue[i] = (long) minValue[i] - 1;
      } else if (type[i] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
        BigDecimal val = (BigDecimal) minValue[i];
        uniqueValue[i] = (val.subtract(new BigDecimal(1.0)));
      } else {
        uniqueValue[i] = (double) minValue[i] - 1;
      }
    }
  }

  /**
   * Below method will be to configure fact file writing configuration
   *
   * @throws CarbonDataWriterException
   */
  private void setWritingConfiguration() throws CarbonDataWriterException {
    // get blocklet size
    this.blockletSize = Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.BLOCKLET_SIZE,
            CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL));
    if (version == ColumnarFormatVersion.V3) {
      this.blockletSize = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE,
              CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT));
    }
    LOGGER.info("Number of rows per column blocklet " + blockletSize);
    dataRows = new ArrayList<>(this.blockletSize);
    int dimSet =
        Integer.parseInt(CarbonCommonConstants.DIMENSION_SPLIT_VALUE_IN_COLUMNAR_DEFAULTVALUE);
    // if atleast one dimension is present then initialize column splitter otherwise null
    int noOfColStore = colGrpModel.getNoOfColumnStore();
    int[] keyBlockSize = new int[noOfColStore + complexColCount];

    if (dimLens.length > 0) {
      //Using Variable length variable split generator
      //This will help in splitting mdkey to columns. variable split is required because all
      // columns which are part of
      //row store will be in single column store
      //e.g if {0,1,2,3,4,5} is dimension and {0,1,2) is row store dimension
      //than below splitter will return column as {0,1,2}{3}{4}{5}
      this.columnarSplitter = this.segmentProperties.getFixedLengthKeySplitter();
      System.arraycopy(columnarSplitter.getBlockKeySize(), 0, keyBlockSize, 0, noOfColStore);
      this.keyBlockHolder =
          new CarbonKeyBlockHolder[this.columnarSplitter.getBlockKeySize().length];
    } else {
      this.keyBlockHolder = new CarbonKeyBlockHolder[0];
    }
    this.complexKeyGenerator = new KeyGenerator[completeDimLens.length];
    for (int i = 0; i < completeDimLens.length; i++) {
      complexKeyGenerator[i] =
          KeyGeneratorFactory.getKeyGenerator(new int[] { completeDimLens[i] });
    }

    for (int i = 0; i < keyBlockHolder.length; i++) {
      this.keyBlockHolder[i] = new CarbonKeyBlockHolder(blockletSize);
      this.keyBlockHolder[i].resetCounter();
    }

    // agg type
    List<Integer> otherMeasureIndexList =
        new ArrayList<Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    List<Integer> customMeasureIndexList =
        new ArrayList<Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (int j = 0; j < type.length; j++) {
      if (type[j] != CarbonCommonConstants.BYTE_VALUE_MEASURE
          && type[j] != CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
        otherMeasureIndexList.add(j);
      } else {
        customMeasureIndexList.add(j);
      }
    }
    otherMeasureIndex = new int[otherMeasureIndexList.size()];
    customMeasureIndex = new int[customMeasureIndexList.size()];
    for (int i = 0; i < otherMeasureIndex.length; i++) {
      otherMeasureIndex[i] = otherMeasureIndexList.get(i);
    }
    for (int i = 0; i < customMeasureIndex.length; i++) {
      customMeasureIndex[i] = customMeasureIndexList.get(i);
    }
    setComplexMapSurrogateIndex(this.dimensionCount);
    int[] blockKeySize = getBlockKeySizeWithComplexTypes(new MultiDimKeyVarLengthEquiSplitGenerator(
        CarbonUtil.getIncrementedCardinalityFullyFilled(completeDimLens.clone()), (byte) dimSet)
        .getBlockKeySize());
    System.arraycopy(blockKeySize, noOfColStore, keyBlockSize, noOfColStore,
        blockKeySize.length - noOfColStore);
    this.dataWriter = getFactDataWriter(keyBlockSize);
    this.dataWriter.setIsNoDictionary(isNoDictionary);
    // initialize the channel;
    this.dataWriter.initializeWriter();
    //initializeColGrpMinMax();
  }

  /**
   * This method combines primitive dimensions with complex metadata columns
   *
   * @param primitiveBlockKeySize
   * @return all dimensions cardinality including complex dimension metadata column
   */
  private int[] getBlockKeySizeWithComplexTypes(int[] primitiveBlockKeySize) {
    int allColsCount = getComplexColsCount();
    int[] blockKeySizeWithComplexTypes =
        new int[this.colGrpModel.getNoOfColumnStore() + allColsCount];

    List<Integer> blockKeySizeWithComplex =
        new ArrayList<Integer>(blockKeySizeWithComplexTypes.length);
    for (int i = 0; i < this.dimensionCount; i++) {
      GenericDataType complexDataType = complexIndexMap.get(i);
      if (complexDataType != null) {
        complexDataType.fillBlockKeySize(blockKeySizeWithComplex, primitiveBlockKeySize);
      } else {
        blockKeySizeWithComplex.add(primitiveBlockKeySize[i]);
      }
    }
    for (int i = 0; i < blockKeySizeWithComplexTypes.length; i++) {
      blockKeySizeWithComplexTypes[i] = blockKeySizeWithComplex.get(i);
    }

    return blockKeySizeWithComplexTypes;
  }

  // TODO Remove after kettle flow got removed.
  private CarbonWriteDataHolder initialiseKeyBlockHolder(int size) {
    CarbonWriteDataHolder keyDataHolder = new CarbonWriteDataHolder();
    keyDataHolder.initialiseByteArrayValues(size);
    return keyDataHolder;
  }

  private CarbonWriteDataHolder initialiseKeyBlockHolderWithOutKettle(int size) {
    CarbonWriteDataHolder keyDataHolder = new CarbonWriteDataHolder();
    keyDataHolder.initialiseByteArrayValuesWithOutKettle(size);
    return keyDataHolder;
  }

  private CarbonWriteDataHolder initialiseKeyBlockHolderForNonDictionary(int size) {
    CarbonWriteDataHolder keyDataHolder = new CarbonWriteDataHolder();
    keyDataHolder.initialiseByteArrayValuesForNonDictionary(size);
    return keyDataHolder;
  }

  private CarbonWriteDataHolder[] initialiseDataHolder(int size) {
    CarbonWriteDataHolder[] dataHolder = new CarbonWriteDataHolder[this.measureCount];
    for (int i = 0; i < otherMeasureIndex.length; i++) {
      dataHolder[otherMeasureIndex[i]] = new CarbonWriteDataHolder();
      if (type[otherMeasureIndex[i]] == CarbonCommonConstants.BIG_INT_MEASURE) {
        dataHolder[otherMeasureIndex[i]].initialiseLongValues(size);
      } else {
        dataHolder[otherMeasureIndex[i]].initialiseDoubleValues(size);
      }
    }
    for (int i = 0; i < customMeasureIndex.length; i++) {
      dataHolder[customMeasureIndex[i]] = new CarbonWriteDataHolder();
      dataHolder[customMeasureIndex[i]].initialiseByteArrayValues(size);
    }
    return dataHolder;
  }

  /**
   * Below method will be used to get the bit set array for
   * all the measure, which will store the indexes which are null
   *
   * @param measureCount
   * @return bit set to store null value index
   */
  private BitSet[] getMeasureNullValueIndexBitSet(int measureCount) {
    BitSet[] nullvalueIndexBitset = new BitSet[measureCount];
    // creating a bit set for all the measure column
    nullvalueIndexBitset = new BitSet[this.measureCount];
    for (int i = 0; i < nullvalueIndexBitset.length; i++) {
      // bitset size will be blocklet size
      nullvalueIndexBitset[i] = new BitSet(this.blockletSize);
    }
    return nullvalueIndexBitset;
  }

  /**
   * Below method will be used to get the fact data writer instance
   *
   * @param keyBlockSize
   * @return data writer instance
   */
  private CarbonFactDataWriter<?> getFactDataWriter(int[] keyBlockSize) {
    return CarbonDataWriterFactory.getInstance()
        .getFactDataWriter(version, getDataWriterVo(keyBlockSize));
  }

  /**
   * Below method will be used to get the writer vo
   *
   * @param keyBlockSize size of each key block
   * @return data writer vo object
   */
  private CarbonDataWriterVo getDataWriterVo(int[] keyBlockSize) {
    CarbonDataWriterVo carbonDataWriterVo = new CarbonDataWriterVo();
    carbonDataWriterVo.setStoreLocation(storeLocation);
    carbonDataWriterVo.setMeasureCount(measureCount);
    carbonDataWriterVo.setMdKeyLength(mdkeyLength);
    carbonDataWriterVo.setTableName(tableName);
    carbonDataWriterVo.setKeyBlockSize(keyBlockSize);
    carbonDataWriterVo.setFileManager(fileManager);
    carbonDataWriterVo.setAggBlocks(aggKeyBlock);
    carbonDataWriterVo.setIsComplexType(isComplexTypes());
    carbonDataWriterVo.setNoDictionaryCount(noDictionaryCount);
    carbonDataWriterVo.setCarbonDataFileAttributes(carbonDataFileAttributes);
    carbonDataWriterVo.setDatabaseName(databaseName);
    carbonDataWriterVo.setWrapperColumnSchemaList(wrapperColumnSchemaList);
    carbonDataWriterVo.setIsDictionaryColumn(dimensionType);
    carbonDataWriterVo.setCarbonDataDirectoryPath(carbonDataDirectoryPath);
    carbonDataWriterVo.setColCardinality(colCardinality);
    carbonDataWriterVo.setSegmentProperties(segmentProperties);
    carbonDataWriterVo.setTableBlocksize(tableBlockSize);
    carbonDataWriterVo.setBucketNumber(bucketNumber);
    carbonDataWriterVo.setTaskExtension(taskExtension);
    carbonDataWriterVo.setSchemaUpdatedTimeStamp(schemaUpdatedTimeStamp);
    return carbonDataWriterVo;
  }

  private boolean[] isComplexTypes() {
    int noOfColumn = colGrpModel.getNoOfColumnStore() + noDictionaryCount + complexIndexMap.size();
    int allColsCount = getColsCount(noOfColumn);
    boolean[] isComplexType = new boolean[allColsCount];

    List<Boolean> complexTypesList = new ArrayList<Boolean>(allColsCount);
    for (int i = 0; i < noOfColumn; i++) {
      GenericDataType complexDataType = complexIndexMap.get(i - noDictionaryCount);
      if (complexDataType != null) {
        int count = complexDataType.getColsCount();
        for (int j = 0; j < count; j++) {
          complexTypesList.add(true);
        }
      } else {
        complexTypesList.add(false);
      }
    }
    for (int i = 0; i < allColsCount; i++) {
      isComplexType[i] = complexTypesList.get(i);
    }

    return isComplexType;
  }

  /**
   * This method will reset the block processing count
   */
  private void resetBlockletProcessingCount() {
    blockletProcessingCount.set(0);
  }

  /**
   * This class will hold the holder objects and manage producer and consumer for reading
   * and writing the blocklet data
   */
  private final class BlockletDataHolder {
    /**
     * array of blocklet data holder objects
     */
    private NodeHolder[] nodeHolders;
    /**
     * flag to check whether the producer has completed processing for holder
     * object which is required to be picked form an index
     */
    private AtomicBoolean available;
    /**
     * index from which data node holder object needs to be picked for writing
     */
    private int currentIndex;

    private BlockletDataHolder() {
      nodeHolders = new NodeHolder[numberOfCores];
      available = new AtomicBoolean(false);
    }

    /**
     * @return a node holder object
     * @throws InterruptedException if consumer thread is interrupted
     */
    public synchronized NodeHolder get() throws InterruptedException {
      NodeHolder nodeHolder = nodeHolders[currentIndex];
      // if node holder is null means producer thread processing the data which has to
      // be inserted at this current index has not completed yet
      if (null == nodeHolder && !processingComplete) {
        available.set(false);
      }
      while (!available.get()) {
        wait();
      }
      nodeHolder = nodeHolders[currentIndex];
      nodeHolders[currentIndex] = null;
      currentIndex++;
      // reset current index when it reaches length of node holder array
      if (currentIndex >= nodeHolders.length) {
        currentIndex = 0;
      }
      return nodeHolder;
    }

    /**
     * @param nodeHolder
     * @param index
     */
    public synchronized void put(NodeHolder nodeHolder, int index) {
      nodeHolders[index] = nodeHolder;
      // notify the consumer thread when index at which object is to be inserted
      // becomes equal to current index from where data has to be picked for writing
      if (index == currentIndex) {
        available.set(true);
        notifyAll();
      }
    }
  }

  /**
   * Producer which will process data equivalent to 1 blocklet size
   */
  private final class Producer implements Callable<Void> {

    private BlockletDataHolder blockletDataHolder;
    private List<Object[]> dataRows;
    private int sequenceNumber;

    private Producer(BlockletDataHolder blockletDataHolder, List<Object[]> dataRows,
        int sequenceNumber) {
      this.blockletDataHolder = blockletDataHolder;
      this.dataRows = dataRows;
      this.sequenceNumber = sequenceNumber;
    }

    /**
     * Computes a result, or throws an exception if unable to do so.
     *
     * @return computed result
     * @throws Exception if unable to compute a result
     */
    @Override public Void call() throws Exception {
      try {
        NodeHolder nodeHolder;
        if (useKettle) {
          nodeHolder = processDataRows(dataRows);
        } else {
          nodeHolder = processDataRowsWithOutKettle(dataRows);
        }
        // insert the object in array according to sequence number
        int indexInNodeHolderArray = (sequenceNumber - 1) % numberOfCores;
        blockletDataHolder.put(nodeHolder, indexInNodeHolderArray);
        return null;
      } catch (Throwable throwable) {
        LOGGER.error(throwable, "Error in producer");
        consumerExecutorService.shutdownNow();
        resetBlockletProcessingCount();
        throw new CarbonDataWriterException(throwable.getMessage(), throwable);
      }
    }
  }

  /**
   * Consumer class will get one blocklet data at a time and submit for writing
   */
  private final class Consumer implements Callable<Void> {

    private BlockletDataHolder blockletDataHolder;

    private Consumer(BlockletDataHolder blockletDataHolder) {
      this.blockletDataHolder = blockletDataHolder;
    }

    /**
     * Computes a result, or throws an exception if unable to do so.
     *
     * @return computed result
     * @throws Exception if unable to compute a result
     */
    @Override public Void call() throws Exception {
      while (!processingComplete || blockletProcessingCount.get() > 0) {
        NodeHolder nodeHolder = null;
        try {
          nodeHolder = blockletDataHolder.get();
          if (null != nodeHolder) {
            dataWriter.writeBlockletData(nodeHolder);
          }
          blockletProcessingCount.decrementAndGet();
        } catch (Throwable throwable) {
          if (!processingComplete || blockletProcessingCount.get() > 0) {
            producerExecutorService.shutdownNow();
            resetBlockletProcessingCount();
            LOGGER.error(throwable, "Problem while writing the carbon data file");
            throw new CarbonDataWriterException(throwable.getMessage());
          }
        } finally {
          semaphore.release();
        }
      }
      return null;
    }
  }

  private final class BlockSortThread implements Callable<IndexStorage> {
    private int index;

    private byte[][] data;
    private boolean isSortRequired;
    private boolean isCompressionReq;
    private boolean isUseInvertedIndex;

    private boolean isNoDictionary;

    private BlockSortThread(int index, byte[][] data, boolean isSortRequired,
        boolean isUseInvertedIndex) {
      this.index = index;
      this.data = data;
      isCompressionReq = aggKeyBlock[this.index];
      this.isSortRequired = isSortRequired;
      this.isUseInvertedIndex = isUseInvertedIndex;
    }

    public BlockSortThread(int index, byte[][] data, boolean b, boolean isNoDictionary,
        boolean isSortRequired, boolean isUseInvertedIndex) {
      this.index = index;
      this.data = data;
      isCompressionReq = b;
      this.isNoDictionary = isNoDictionary;
      this.isSortRequired = isSortRequired;
      this.isUseInvertedIndex = isUseInvertedIndex;
    }

    @Override public IndexStorage call() throws Exception {
      if (isUseInvertedIndex) {
        if (version == ColumnarFormatVersion.V3) {
          return new BlockIndexerStorageForShort(this.data, isCompressionReq, isNoDictionary,
              isSortRequired);
        } else {
          return new BlockIndexerStorageForInt(this.data, isCompressionReq, isNoDictionary,
              isSortRequired);
        }
      } else {
        if (version == ColumnarFormatVersion.V3) {
          return new BlockIndexerStorageForNoInvertedIndexForShort(this.data, isNoDictionary);
        } else {
          return new BlockIndexerStorageForNoInvertedIndex(this.data, isNoDictionary);
        }
      }

    }

  }
}
