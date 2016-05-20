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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.metadata.CarbonMetadata;
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.columnar.BlockIndexerStorageForInt;
import org.carbondata.core.datastorage.store.columnar.IndexStorage;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.datastorage.store.dataholder.CarbonWriteDataHolder;
import org.carbondata.core.datastorage.util.StoreFactory;
import org.carbondata.core.file.manager.composite.FileData;
import org.carbondata.core.file.manager.composite.IFileManagerComposite;
import org.carbondata.core.file.manager.composite.LoadFolderData;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.columnar.ColumnarSplitter;
import org.carbondata.core.keygenerator.columnar.impl.MultiDimKeyVarLengthEquiSplitGenerator;
import org.carbondata.core.keygenerator.columnar.impl.MultiDimKeyVarLengthVariableSplitGenerator;
import org.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.util.DataTypeUtil;
import org.carbondata.core.util.ValueCompressionUtil;
import org.carbondata.core.vo.ColumnGroupModel;
import org.carbondata.processing.datatypes.GenericDataType;
import org.carbondata.processing.store.colgroup.ColumnDataHolder;
import org.carbondata.processing.store.colgroup.DataHolder;
import org.carbondata.processing.store.colgroup.RowBlockStorage;
import org.carbondata.processing.store.colgroup.RowStoreDataHolder;
import org.carbondata.processing.store.writer.CarbonFactDataWriter;
import org.carbondata.processing.store.writer.CarbonFactDataWriterImplForIntIndexAndAggBlock;
import org.carbondata.processing.store.writer.exception.CarbonDataWriterException;
import org.carbondata.processing.util.RemoveDictionaryUtil;

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
   * decimalPointers
   */
  private final byte decimalPointers = Byte.parseByte(CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_DECIMAL_POINTERS,
          CarbonCommonConstants.CARBON_DECIMAL_POINTERS_DEFAULT));
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
  /**
   * startkey of each blocklet
   */
  private byte[] startKey;
  /**
   * no dictionary start key.
   */
  private byte[] noDictStartKey;
  /**
   * end key of each blocklet
   */
  private byte[] endKey;
  /**
   * no dictionary Endkey.
   */
  private byte[] noDictEndKey;
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
   * CarbonWriteDataHolder
   */
  private CarbonWriteDataHolder[] dataHolder;
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
  private KeyGenerator[] complexKeyGenerator;
  /**
   * maskedByteRanges
   */
  private int[] maskedByte;
  /**
   * isDataWritingRequest
   */
  //    private boolean isDataWritingRequest;

  private Object lock = new Object();
  private CarbonWriteDataHolder keyDataHolder;
  private CarbonWriteDataHolder NoDictionarykeyDataHolder;
  private int noDictionaryCount;
  private ColumnGroupModel colGrpModel;
  private int[] primitiveDimLens;
  private char[] type;
  private int[] completeDimLens;
  private Object[] max;
  private Object[] min;
  private int[] decimal;
  //TODO: To be removed after presence meta is introduced.
  private Object[] uniqueValue;
  /**
   * data file attributes which will used for file construction
   */
  private CarbonDataFileAttributes carbonDataFileAttributes;
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
   * CarbonFactDataHandler constructor
   */
  public CarbonFactDataHandlerColumnar(CarbonFactDataHandlerModel carbonFactDataHandlerModel) {
    initParameters(carbonFactDataHandlerModel);
    this.dimensionCount = carbonFactDataHandlerModel.getDimensionCount();
    this.complexIndexMap = carbonFactDataHandlerModel.getComplexIndexMap();
    this.primitiveDimLens = carbonFactDataHandlerModel.getPrimitiveDimLens();
    this.isAggKeyBlock = Boolean.parseBoolean(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.AGGREAGATE_COLUMNAR_KEY_BLOCK,
            CarbonCommonConstants.AGGREAGATE_COLUMNAR_KEY_BLOCK_DEFAULTVALUE));

    this.complexColCount = getComplexColsCount();
    this.columnStoreCount =
        this.colGrpModel.getNoOfColumnStore() + noDictionaryCount + complexColCount;

    this.aggKeyBlock = new boolean[columnStoreCount];
    this.isNoDictionary = new boolean[columnStoreCount];
    int noDictStartIndex = this.colGrpModel.getNoOfColumnStore();
    // setting true value for dims of high card
    for (int i = 0; i < noDictionaryCount; i++) {
      this.isNoDictionary[noDictStartIndex + i] = true;
    }

    // setting true value for dims of high card
    for (int i = noDictStartIndex; i < isNoDictionary.length; i++) {
      this.isNoDictionary[i] = true;
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
  }

  private void initParameters(CarbonFactDataHandlerModel carbonFactDataHandlerModel) {
    this.databaseName = carbonFactDataHandlerModel.getDatabaseName();
    this.tableName = carbonFactDataHandlerModel.getTableName();
    this.storeLocation = carbonFactDataHandlerModel.getStoreLocation();
    this.measureCount = carbonFactDataHandlerModel.getMeasureCount();
    this.mdkeyLength = carbonFactDataHandlerModel.getMdKeyLength();
    this.mdKeyIndex = carbonFactDataHandlerModel.getMdKeyIndex();
    this.noDictionaryCount = carbonFactDataHandlerModel.getNoDictionaryCount();
    this.colGrpModel = carbonFactDataHandlerModel.getColGrpModel();
    this.completeDimLens = carbonFactDataHandlerModel.getDimLens();
    this.dimLens = colGrpModel.getColumnGroupCardinality();
    this.carbonDataFileAttributes = carbonFactDataHandlerModel.getCarbonDataFileAttributes();
    this.type = carbonFactDataHandlerModel.getAggType();
    LOGGER.info("Initializing writer executers");
    //TODO need to pass carbon table identifier to metadata
    CarbonTable carbonTable =
        CarbonMetadata.getInstance().getCarbonTable(databaseName + '_' + tableName);
    fillColumnSchemaList(carbonTable.getDimensionByTableName(tableName),
        carbonTable.getMeasureByTableName(tableName));
    dimensionType =
        CarbonUtil.identifyDimensionType(carbonTable.getDimensionByTableName(tableName));
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

  private void fillColumnSchemaList(List<CarbonDimension> carbonDimensionsList,
      List<CarbonMeasure> carbonMeasureList) {
    wrapperColumnSchemaList = new ArrayList<ColumnSchema>();
    for (CarbonDimension carbonDimension : carbonDimensionsList) {
      wrapperColumnSchemaList.add(carbonDimension.getColumnSchema());
    }
    for (CarbonMeasure carbonMeasure : carbonMeasureList) {
      wrapperColumnSchemaList.add(carbonMeasure.getColumnSchema());
    }
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
    fileManager = new LoadFolderData();
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
    byte[] mdkey = (byte[]) row[this.mdKeyIndex];
    byte[] noDictionaryKey = null;
    if (noDictionaryCount > 0 || complexIndexMap.size() > 0) {
      noDictionaryKey = (byte[]) row[this.mdKeyIndex - 1];
    }
    ByteBuffer byteBuffer = null;
    byte[] b = null;
    if (this.entryCount == 0) {
      this.startKey = mdkey;
      this.noDictStartKey = noDictionaryKey;
    }
    this.endKey = mdkey;
    this.noDictEndKey = noDictionaryKey;
    // add to key store
    if (mdkey.length > 0) {
      keyDataHolder.setWritableByteArrayValueByIndex(entryCount, mdkey);
    }

    // for storing the byte [] for high card.
    if (noDictionaryCount > 0 || complexIndexMap.size() > 0) {
      NoDictionarykeyDataHolder.setWritableByteArrayValueByIndex(entryCount, noDictionaryKey);
    }
    //Add all columns to keyDataHolder
    keyDataHolder.setWritableByteArrayValueByIndex(entryCount, this.mdKeyIndex, row);

    // CHECKSTYLE:OFF Approval No:Approval-351
    for (int k = 0; k < otherMeasureIndex.length; k++) {
      if (type[otherMeasureIndex[k]] == CarbonCommonConstants.BIG_INT_MEASURE) {
        if (null == row[otherMeasureIndex[k]]) {
          //TODO: Not handling unique key as it will be handled in presence data
          dataHolder[otherMeasureIndex[k]].setWritableLongValueByIndex(entryCount, 0);
        } else {
          dataHolder[otherMeasureIndex[k]]
              .setWritableLongValueByIndex(entryCount, row[otherMeasureIndex[k]]);
        }
      } else {
        if (null == row[otherMeasureIndex[k]]) {
          //TODO: Not handling unique key as it will be handled in presence data
          dataHolder[otherMeasureIndex[k]].setWritableDoubleValueByIndex(entryCount, 0);
        } else {
          dataHolder[otherMeasureIndex[k]]
              .setWritableDoubleValueByIndex(entryCount, row[otherMeasureIndex[k]]);
        }
      }
    }
    calculateMaxMinUnique(max, min, decimal, otherMeasureIndex, row);
    for (int i = 0; i < customMeasureIndex.length; i++) {
      if (null == row[customMeasureIndex[i]]
          && type[customMeasureIndex[i]] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
        //TODO: Not handling unique key as it will be handled in presence data
        BigDecimal val = BigDecimal.valueOf(0);
        b = DataTypeUtil.bigDecimalToByte(val);
      } else {
        b = (byte[]) row[customMeasureIndex[i]];
      }
      byteBuffer = ByteBuffer.allocate(b.length + CarbonCommonConstants.INT_SIZE_IN_BYTE);
      byteBuffer.putInt(b.length);
      byteBuffer.put(b);
      byteBuffer.flip();
      b = byteBuffer.array();
      dataHolder[customMeasureIndex[i]].setWritableByteArrayValueByIndex(entryCount, b);
    }
    calculateMaxMinUnique(max, min, decimal, customMeasureIndex, row);
    this.entryCount++;
    // if entry count reaches to blocklet size then we are ready to write
    // this to data file and update the intermediate files
    if (this.entryCount == this.blockletSize) {
      byte[][] byteArrayValues = keyDataHolder.getByteArrayValues().clone();
      byte[][] noDictionaryValueHolder = NoDictionarykeyDataHolder.getByteArrayValues();
      //TODO need to handle high card also here
      calculateUniqueValue(min, uniqueValue);
      ValueCompressionModel compressionModel = ValueCompressionUtil
          .getValueCompressionModel(max, min, decimal, uniqueValue, type, new byte[max.length]);
      byte[][] writableMeasureDataArray =
          StoreFactory.createDataStore(compressionModel).getWritableMeasureDataArray(dataHolder)
              .clone();
      initializeMinMax();
      int entryCountLocal = entryCount;
      byte[] startKeyLocal = startKey;
      byte[] endKeyLocal = endKey;
      startKey = new byte[mdkeyLength];
      endKey = new byte[mdkeyLength];

      DataWriterThread dataWriterThread =
          new DataWriterThread(writableMeasureDataArray, byteArrayValues, entryCountLocal,
              startKeyLocal, endKeyLocal, compressionModel, noDictionaryValueHolder, noDictStartKey,
              noDictEndKey);
      try {
        dataWriterThread.call();
      } catch (Exception e) {
        throw new CarbonDataWriterException(
            "Problem while writing the data to carbon data file: " + e);
      }
      // set the entry count to zero
      processedDataCount += entryCount;
      LOGGER.info("*******************************************Number Of records processed: "
          + processedDataCount);
      this.entryCount = 0;
      resetKeyBlockHolder();
      initialisedataHolder();
      keyDataHolder.reset();
    }
  }

  private void writeDataToFile(byte[][] dataHolderLocal, byte[][] byteArrayValues,
      int entryCountLocal, byte[] startkeyLocal, byte[] endKeyLocal,
      ValueCompressionModel compressionModel, byte[][] noDictionaryData,
      byte[] noDictionaryStartKey, byte[] noDictionaryEndKey) throws CarbonDataWriterException {
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
        byte[][] splitKey = RemoveDictionaryUtil
            .splitNoDictionaryKey(noDictionaryData[i], noDictionaryCount + complexIndexMap.size());

        int complexTypeIndex = 0;
        for (int j = 0; j < splitKey.length; j++) {
          //nodictionary Columns
          if (j < noDictionaryCount) {
            noDictionaryColumnsData[j][i] = splitKey[j];
          }
          //complex types
          else {
            //Need to write columnar block from complex byte array
            GenericDataType complexDataType = complexIndexMap.get(complexColumnIndex++);
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
    ExecutorService executorService = Executors.newFixedThreadPool(7);
    List<Future<IndexStorage>> submit = new ArrayList<Future<IndexStorage>>(
        primitiveDimLens.length + noDictionaryCount + complexColCount);
    int i = 0;
    int dictionaryColumnCount = -1;
    int noDictionaryColumnCount = -1;
    for (i = 0; i < dimensionType.length; i++) {
      if (dimensionType[i]) {
        dictionaryColumnCount++;
        if (colGrpModel.isColumnar(dictionaryColumnCount)) {
          submit.add(executorService
              .submit(new BlockSortThread(i, dataHolders[dictionaryColumnCount].getData(), true)));
        } else {
          submit
              .add(executorService.submit(new RowBlockStorage(dataHolders[dictionaryColumnCount])));
        }
      } else {
        submit.add(executorService.submit(
            new BlockSortThread(i, noDictionaryColumnsData[++noDictionaryColumnCount], false, true,
                true)));
      }
    }
    for (int k = 0; k < complexColCount; k++) {
      submit.add(executorService.submit(new BlockSortThread(i++,
          colsAndValues.get(k).toArray(new byte[colsAndValues.get(k).size()][]), false)));
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
    //    blockStorage=arrangeDimensionColumnsData(blockStorage);
    synchronized (lock) {
      this.dataWriter.writeDataToFile(blockStorage, dataHolderLocal, entryCountLocal, startkeyLocal,
          endKeyLocal, compressionModel, noDictionaryStartKey, noDictionaryEndKey);
    }
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
    for (int i = 0; i < noOfColumn; i++) {
      if (colGrpModel.isColumnar(i)) {
        dataHolders[i] = new ColumnDataHolder(noOfRow);
      } else {
        dataHolders[i] =
            new RowStoreDataHolder(this.colGrpModel, this.columnarSplitter, i, noOfRow);
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
    // / still some data is present in stores if entryCount is more
    // than 0
    if (this.entryCount > 0) {
      byte[][] data = keyDataHolder.getByteArrayValues();
      calculateUniqueValue(min, uniqueValue);
      ValueCompressionModel compressionModel = ValueCompressionUtil
          .getValueCompressionModel(max, min, decimal, uniqueValue, type, new byte[max.length]);
      byte[][] writableMeasureDataArray =
          StoreFactory.createDataStore(compressionModel).getWritableMeasureDataArray(dataHolder);
      writeDataToFile(writableMeasureDataArray, data, entryCount, this.startKey, this.endKey,
          compressionModel, NoDictionarykeyDataHolder.getByteArrayValues(), this.noDictStartKey,
          this.noDictEndKey);
      processedDataCount += entryCount;
      LOGGER.info("*******************************************Number Of records processed: "
          + processedDataCount);
      this.dataWriter.writeleafMetaDataToFile();
    } else if (null != this.dataWriter) {
      this.dataWriter.writeleafMetaDataToFile();
    }
  }

  private byte[] getAggregateTableMdkey(byte[] maksedKey) throws CarbonDataWriterException {
    long[] keyArray = this.factKeyGenerator.getKeyArray(maksedKey, maskedByte);

    int[] aggSurrogateKey = new int[surrogateIndex.length];

    for (int j = 0; j < aggSurrogateKey.length; j++) {
      aggSurrogateKey[j] = (int) keyArray[surrogateIndex[j]];
    }

    try {
      return keyGenerator.generateKey(aggSurrogateKey);
    } catch (KeyGenException e) {
      throw new CarbonDataWriterException("Problem while generating the mdkeyfor aggregate ", e);
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
  public void closeHandler() {
    if (null != this.dataWriter) {
      // close all the open stream for both the files
      this.dataWriter.closeWriter();
      int size = fileManager.size();
      FileData fileData = null;
      String storePath = null;
      String inProgFileName = null;
      String changedFileName = null;
      File currntFile = null;
      File destFile = null;
      for (int i = 0; i < size; i++) {
        fileData = (FileData) fileManager.get(i);

        storePath = fileData.getStorePath();
        inProgFileName = fileData.getFileName();
        changedFileName = inProgFileName.substring(0, inProgFileName.lastIndexOf('.'));
        currntFile = new File(storePath + File.separator + inProgFileName);
        destFile = new File(storePath + File.separator + changedFileName);
        if (!currntFile.renameTo(destFile)) {
          LOGGER.info("Problem while renaming the file");
        }
        fileData.setName(changedFileName);
      }
    }
    this.dataWriter = null;
    this.keyBlockHolder = null;
  }

  /**
   * This method will be used to update the max value for each measure
   */
  private void calculateMaxMinUnique(Object[] max, Object[] min, int[] decimal, int[] msrIndex,
      Object[] row) {
    // Update row level min max
    for (int i = 0; i < msrIndex.length; i++) {
      int count = msrIndex[i];
      if (type[count] == CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE) {
        double value = (double) row[count];
        double maxVal = (double) max[count];
        double minVal = (double) min[count];
        max[count] = (maxVal > value ? max[count] : value);
        min[count] = (minVal < value ? min[count] : value);
        int num = (value % 1 == 0) ? 0 : decimalPointers;
        decimal[count] = (decimal[count] > num ? decimal[count] : num);
      } else if (type[count] == CarbonCommonConstants.BIG_INT_MEASURE) {
        long value = (long) row[count];
        long maxVal = (long) max[count];
        long minVal = (long) min[count];
        max[count] = (maxVal > value ? max[count] : value);
        min[count] = (minVal < value ? min[count] : value);
        int num = (value % 1 == 0) ? 0 : decimalPointers;
        decimal[count] = (decimal[count] > num ? decimal[count] : num);
      } else if (type[count] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
        BigDecimal value = (BigDecimal) row[count];
        BigDecimal minVal = (BigDecimal) min[count];
        min[count] = minVal.min(value);
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

    LOGGER.info("************* Blocklet Size: " + blockletSize);

    int dimSet =
        Integer.parseInt(CarbonCommonConstants.DIMENSION_SPLIT_VALUE_IN_COLUMNAR_DEFAULTVALUE);
    // if atleast one dimension is present then initialize column splitter otherwise null

    int[] keyBlockSize = null;
    if (dimLens.length > 0) {
      //Using Variable length variable split generator
      //This will help in splitting mdkey to columns. variable split is required because all
      // columns which are part of
      //row store will be in single column store
      //e.g if {0,1,2,3,4,5} is dimension and {0,1,2) is row store dimension
      //than below splitter will return column as {0,1,2}{3}{4}{5}
      this.columnarSplitter = new MultiDimKeyVarLengthVariableSplitGenerator(CarbonUtil
          .getDimensionBitLength(colGrpModel.getColumnGroupCardinality(),
              colGrpModel.getColumnSplit()), colGrpModel.getColumnSplit());
      this.keyBlockHolder =
          new CarbonKeyBlockHolder[this.columnarSplitter.getBlockKeySize().length];
      keyBlockSize = columnarSplitter.getBlockKeySize();
      this.complexKeyGenerator = new KeyGenerator[completeDimLens.length];
      for (int i = 0; i < completeDimLens.length; i++) {
        complexKeyGenerator[i] =
            KeyGeneratorFactory.getKeyGenerator(new int[] { completeDimLens[i] });
      }
    } else {
      keyBlockSize = new int[0];
      this.keyBlockHolder = new CarbonKeyBlockHolder[0];
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
      if (type[j] != 'c') {
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

    this.dataHolder = new CarbonWriteDataHolder[this.measureCount];
    for (int i = 0; i < otherMeasureIndex.length; i++) {
      this.dataHolder[otherMeasureIndex[i]] = new CarbonWriteDataHolder();
      if (type[otherMeasureIndex[i]] == CarbonCommonConstants.BIG_INT_MEASURE) {
        this.dataHolder[otherMeasureIndex[i]].initialiseLongValues(this.blockletSize);
      } else {
        this.dataHolder[otherMeasureIndex[i]].initialiseDoubleValues(this.blockletSize);
      }
    }
    for (int i = 0; i < customMeasureIndex.length; i++) {
      this.dataHolder[customMeasureIndex[i]] = new CarbonWriteDataHolder();
      this.dataHolder[customMeasureIndex[i]].initialiseByteArrayValues(blockletSize);
    }

    keyDataHolder = new CarbonWriteDataHolder();
    keyDataHolder.initialiseByteArrayValues(blockletSize);
    NoDictionarykeyDataHolder = new CarbonWriteDataHolder();
    NoDictionarykeyDataHolder.initialiseByteArrayValues(blockletSize);

    initialisedataHolder();
    setComplexMapSurrogateIndex(this.dimensionCount);
    int[] blockKeySize = getBlockKeySizeWithComplexTypes(new MultiDimKeyVarLengthEquiSplitGenerator(
        CarbonUtil.getIncrementedCardinalityFullyFilled(completeDimLens.clone()), (byte) dimSet)
        .getBlockKeySize());
    this.dataWriter =
        getFactDataWriter(this.storeLocation, this.measureCount, this.mdkeyLength, this.tableName,
            true, fileManager, blockKeySize);
    this.dataWriter.setIsNoDictionary(isNoDictionary);
    // initialize the channel;
    this.dataWriter.initializeWriter();

    initializeMinMax();
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

  //TODO: Need to move Abstract class
  private void initializeMinMax() {
    max = new Object[measureCount];
    min = new Object[measureCount];
    decimal = new int[measureCount];
    uniqueValue = new Object[measureCount];

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
  }

  private void resetKeyBlockHolder() {
    for (int i = 0; i < keyBlockHolder.length; i++) {
      this.keyBlockHolder[i].resetCounter();
    }
  }

  private void initialisedataHolder() {
    for (int i = 0; i < this.dataHolder.length; i++) {
      this.dataHolder[i].reset();
    }

  }

  private CarbonFactDataWriter<?> getFactDataWriter(String storeLocation, int measureCount,
      int mdKeyLength, String tableName, boolean isNodeHolder, IFileManagerComposite fileManager,
      int[] keyBlockSize) {
    return new CarbonFactDataWriterImplForIntIndexAndAggBlock(storeLocation, measureCount,
        mdKeyLength, tableName, isNodeHolder, fileManager, keyBlockSize, aggKeyBlock, false,
        isComplexTypes(), noDictionaryCount, carbonDataFileAttributes, databaseName,
        wrapperColumnSchemaList, noDictionaryCount, dimensionType);
  }

  private boolean[] isComplexTypes() {
    int noOfColumn = colGrpModel.getNoOfColumnStore() + noDictionaryCount + complexIndexMap.size();
    int allColsCount = getColsCount(noOfColumn);
    boolean[] isComplexType = new boolean[allColsCount];

    List<Boolean> complexTypesList = new ArrayList<Boolean>(allColsCount);
    for (int i = 0; i < noOfColumn; i++) {
      GenericDataType complexDataType = complexIndexMap.get(i);
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

  private final class DataWriterThread implements Callable<IndexStorage> {
    private byte[][] noDictionaryValueHolder;

    private byte[][] keyByteArrayValues;

    private byte[][] dataHolderLocal;

    private int entryCountLocal;

    private byte[] startkeyLocal;

    private byte[] endKeyLocal;

    private byte[] noDictStartKeyLocal;

    private byte[] noDictEndKeyLocal;

    private ValueCompressionModel compressionModel;

    private DataWriterThread(byte[][] dataHolderLocal, byte[][] keyByteArrayValues,
        int entryCountLocal, byte[] startKey, byte[] endKey, ValueCompressionModel compressionModel,
        byte[][] noDictionaryValueHolder, byte[] noDictStartKey, byte[] noDictEndKey) {
      this.keyByteArrayValues = keyByteArrayValues;
      this.entryCountLocal = entryCountLocal;
      this.startkeyLocal = startKey;
      this.endKeyLocal = endKey;
      this.dataHolderLocal = dataHolderLocal;
      this.compressionModel = compressionModel;
      this.noDictionaryValueHolder = noDictionaryValueHolder;
      this.noDictStartKeyLocal = noDictStartKey;
      this.noDictEndKeyLocal = noDictEndKey;
    }

    @Override public IndexStorage call() throws Exception {
      writeDataToFile(dataHolderLocal, keyByteArrayValues, entryCountLocal, startkeyLocal,
          endKeyLocal, compressionModel, noDictionaryValueHolder, noDictStartKeyLocal,
          noDictEndKeyLocal);
      return null;
    }

  }

  private final class BlockSortThread implements Callable<IndexStorage> {
    private int index;

    private byte[][] data;
    private boolean isSortRequired;
    private boolean isCompressionReq;

    private boolean isNoDictionary;

    private BlockSortThread(int index, byte[][] data, boolean isSortRequired) {
      this.index = index;
      this.data = data;
      isCompressionReq = aggKeyBlock[this.index];
      this.isSortRequired = isSortRequired;
    }

    public BlockSortThread(int index, byte[][] data, boolean b, boolean isNoDictionary,
        boolean isSortRequired) {
      this.index = index;
      this.data = data;
      isCompressionReq = b;
      this.isNoDictionary = isNoDictionary;
      this.isSortRequired = isSortRequired;
    }

    @Override public IndexStorage call() throws Exception {
      return new BlockIndexerStorageForInt(this.data, isCompressionReq, isNoDictionary,
          isSortRequired);

    }

  }
}
