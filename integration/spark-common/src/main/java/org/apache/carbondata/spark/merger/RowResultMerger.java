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
package org.apache.carbondata.spark.merger;

import java.io.File;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.scan.result.iterator.RawResultIterator;
import org.apache.carbondata.core.scan.wrappers.ByteArrayWrapper;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.datatypes.GenericDataType;
import org.apache.carbondata.processing.model.CarbonLoadModel;
import org.apache.carbondata.processing.store.CarbonDataFileAttributes;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerColumnar;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;
import org.apache.carbondata.processing.store.CarbonFactHandler;
import org.apache.carbondata.processing.store.writer.exception.CarbonDataWriterException;
import org.apache.carbondata.spark.merger.exeception.SliceMergerException;

/**
 * This is the Merger class responsible for the merging of the segments.
 */
public class RowResultMerger {

  private final String databaseName;
  private final String tableName;
  private final String tempStoreLocation;
  private final String factStoreLocation;
  private CarbonFactHandler dataHandler;
  private List<RawResultIterator> rawResultIteratorList =
      new ArrayList<RawResultIterator>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  private SegmentProperties segprop;
  /**
   * record holder heap
   */
  private AbstractQueue<RawResultIterator> recordHolderHeap;

  private TupleConversionAdapter tupleConvertor;

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(RowResultMerger.class.getName());

  public RowResultMerger(List<RawResultIterator> iteratorList, String databaseName,
      String tableName, SegmentProperties segProp, String tempStoreLocation,
      CarbonLoadModel loadModel, CompactionType compactionType) {

    CarbonDataFileAttributes carbonDataFileAttributes;

    this.rawResultIteratorList = iteratorList;
    // create the List of RawResultIterator.

    recordHolderHeap = new PriorityQueue<RawResultIterator>(rawResultIteratorList.size(),
        new RowResultMerger.CarbonMdkeyComparator());

    this.segprop = segProp;
    this.tempStoreLocation = tempStoreLocation;

    this.factStoreLocation = loadModel.getStorePath();

    if (!new File(tempStoreLocation).mkdirs()) {
      LOGGER.error("Error while new File(tempStoreLocation).mkdirs() ");
    }

    this.databaseName = databaseName;
    this.tableName = tableName;

    int measureCount = segprop.getMeasures().size();
    CarbonTable carbonTable = CarbonMetadata.getInstance()
            .getCarbonTable(databaseName + CarbonCommonConstants.UNDERSCORE + tableName);
    CarbonFactDataHandlerModel carbonFactDataHandlerModel =
        getCarbonFactDataHandlerModel(loadModel);
    carbonFactDataHandlerModel.setPrimitiveDimLens(segprop.getDimColumnsCardinality());

    if (compactionType == CompactionType.IUD_UPDDEL_DELTA_COMPACTION) {
      int taskNo = CarbonUpdateUtil.getLatestTaskIdForSegment(loadModel.getSegmentId(),
          CarbonStorePath.getCarbonTablePath(loadModel.getStorePath(),
              carbonTable.getCarbonTableIdentifier()));

      // Increase the Task Index as in IUD_UPDDEL_DELTA_COMPACTION the new file will
      // be written in same segment. So the TaskNo should be incremented by 1 from max val.
      int index = taskNo + 1;
      carbonDataFileAttributes = new CarbonDataFileAttributes(index, loadModel.getFactTimeStamp());
    }
    else {
      carbonDataFileAttributes =
          new CarbonDataFileAttributes(Integer.parseInt(loadModel.getTaskNo()),
              loadModel.getFactTimeStamp());
    }

    carbonFactDataHandlerModel.setCarbonDataFileAttributes(carbonDataFileAttributes);
    if (segProp.getNumberOfNoDictionaryDimension() > 0
        || segProp.getComplexDimensions().size() > 0) {
      carbonFactDataHandlerModel.setMdKeyIndex(measureCount + 1);
    } else {
      carbonFactDataHandlerModel.setMdKeyIndex(measureCount);
    }
    carbonFactDataHandlerModel.setBlockSizeInMB(carbonTable.getBlockSizeInMB());
    dataHandler = new CarbonFactDataHandlerColumnar(carbonFactDataHandlerModel);

    tupleConvertor = new TupleConversionAdapter(segProp);
  }

  /**
   * Merge function
   *
   */
  public boolean mergerSlice() {
    boolean mergeStatus = false;
    int index = 0;
    boolean isDataPresent = false;
    try {

      // add all iterators to the queue
      for (RawResultIterator leaftTupleIterator : this.rawResultIteratorList) {
        this.recordHolderHeap.add(leaftTupleIterator);
        index++;
      }
      RawResultIterator iterator = null;
      while (index > 1) {
        // iterator the top record
        iterator = this.recordHolderHeap.poll();
        Object[] convertedRow = iterator.next();
        if (null == convertedRow) {
          index--;
          continue;
        }
        if (!isDataPresent) {
          dataHandler.initialise();
          isDataPresent = true;
        }
        // get the mdkey
        addRow(convertedRow);
        // if there is no record in the leaf and all then decrement the
        // index
        if (!iterator.hasNext()) {
          index--;
          continue;
        }
        // add record to heap
        this.recordHolderHeap.add(iterator);
      }
      // if record holder is not empty then iterator the slice holder from
      // heap
      iterator = this.recordHolderHeap.poll();
      while (true) {
        Object[] convertedRow = iterator.next();
        if (null == convertedRow) {
          break;
        }
        // do it only once
        if (!isDataPresent) {
          dataHandler.initialise();
          isDataPresent = true;
        }
        addRow(convertedRow);
        // check if leaf contains no record
        if (!iterator.hasNext()) {
          break;
        }
      }
      if (isDataPresent)
      {
        this.dataHandler.finish();
      }
      mergeStatus = true;
    } catch (Exception e) {
      LOGGER.error(e, e.getMessage());
      LOGGER.error("Exception in compaction merger " + e.getMessage());
      mergeStatus = false;
    } finally {
      try {
        if (isDataPresent) {
          this.dataHandler.closeHandler();
        }
      } catch (CarbonDataWriterException e) {
        LOGGER.error("Exception while closing the handler in compaction merger " + e.getMessage());
        mergeStatus = false;
      }
    }

    return mergeStatus;
  }

  /**
   * Below method will be used to add sorted row
   *
   * @throws SliceMergerException
   */
  private void addRow(Object[] carbonTuple) throws SliceMergerException {
    Object[] rowInWritableFormat;

    rowInWritableFormat = tupleConvertor.getObjectArray(carbonTuple);
    try {
      this.dataHandler.addDataToStore(rowInWritableFormat);
    } catch (CarbonDataWriterException e) {
      throw new SliceMergerException("Problem in merging the slice", e);
    }
  }

  /**
   * This method will create a model object for carbon fact data handler
   *
   * @param loadModel
   * @return
   */
  private CarbonFactDataHandlerModel getCarbonFactDataHandlerModel(CarbonLoadModel loadModel) {
    CarbonFactDataHandlerModel carbonFactDataHandlerModel = new CarbonFactDataHandlerModel();
    carbonFactDataHandlerModel.setDatabaseName(databaseName);
    carbonFactDataHandlerModel.setTableName(tableName);
    carbonFactDataHandlerModel.setMeasureCount(segprop.getMeasures().size());
    carbonFactDataHandlerModel.setCompactionFlow(true);
    carbonFactDataHandlerModel
        .setMdKeyLength(segprop.getDimensionKeyGenerator().getKeySizeInBytes());
    carbonFactDataHandlerModel.setStoreLocation(tempStoreLocation);
    carbonFactDataHandlerModel.setDimLens(segprop.getDimColumnsCardinality());
    carbonFactDataHandlerModel.setSegmentProperties(segprop);
    carbonFactDataHandlerModel.setNoDictionaryCount(segprop.getNumberOfNoDictionaryDimension());
    carbonFactDataHandlerModel.setDimensionCount(
        segprop.getDimensions().size() - carbonFactDataHandlerModel.getNoDictionaryCount());
    CarbonTable carbonTable = CarbonMetadata.getInstance()
        .getCarbonTable(databaseName + CarbonCommonConstants.UNDERSCORE + tableName);
    List<ColumnSchema> wrapperColumnSchema = CarbonUtil
        .getColumnSchemaList(carbonTable.getDimensionByTableName(tableName),
            carbonTable.getMeasureByTableName(tableName));
    carbonFactDataHandlerModel.setWrapperColumnSchema(wrapperColumnSchema);
    // get the cardinality for all all the columns including no dictionary columns
    int[] formattedCardinality =
        CarbonUtil.getFormattedCardinality(segprop.getDimColumnsCardinality(), wrapperColumnSchema);
    carbonFactDataHandlerModel.setColCardinality(formattedCardinality);
    //TO-DO Need to handle complex types here .
    Map<Integer, GenericDataType> complexIndexMap =
        new HashMap<Integer, GenericDataType>(segprop.getComplexDimensions().size());
    carbonFactDataHandlerModel.setComplexIndexMap(complexIndexMap);
    carbonFactDataHandlerModel.setDataWritingRequest(true);

    char[] aggType = new char[segprop.getMeasures().size()];
    Arrays.fill(aggType, 'n');
    int i = 0;
    for (CarbonMeasure msr : segprop.getMeasures()) {
      aggType[i++] = DataTypeUtil.getAggType(msr.getDataType());
    }
    carbonFactDataHandlerModel.setAggType(aggType);
    carbonFactDataHandlerModel.setFactDimLens(segprop.getDimColumnsCardinality());

    String carbonDataDirectoryPath =
        checkAndCreateCarbonStoreLocation(this.factStoreLocation, databaseName, tableName,
            loadModel.getPartitionId(), loadModel.getSegmentId());
    carbonFactDataHandlerModel.setCarbonDataDirectoryPath(carbonDataDirectoryPath);

    List<CarbonDimension> dimensionByTableName =
        loadModel.getCarbonDataLoadSchema().getCarbonTable().getDimensionByTableName(tableName);
    boolean[] isUseInvertedIndexes = new boolean[dimensionByTableName.size()];
    int index = 0;
    for (CarbonDimension dimension : dimensionByTableName) {
      isUseInvertedIndexes[index++] = dimension.isUseInvertedIndex();
    }
    carbonFactDataHandlerModel.setIsUseInvertedIndex(isUseInvertedIndexes);
    return carbonFactDataHandlerModel;
  }

  /**
   * This method will get the store location for the given path, segment id and partition id
   *
   * @return data directory path
   */
  private String checkAndCreateCarbonStoreLocation(String factStoreLocation, String databaseName,
      String tableName, String partitionId, String segmentId) {
    CarbonTable carbonTable = CarbonMetadata.getInstance()
        .getCarbonTable(databaseName + CarbonCommonConstants.UNDERSCORE + tableName);
    CarbonTableIdentifier carbonTableIdentifier = carbonTable.getCarbonTableIdentifier();
    CarbonTablePath carbonTablePath =
        CarbonStorePath.getCarbonTablePath(factStoreLocation, carbonTableIdentifier);
    String carbonDataDirectoryPath =
        carbonTablePath.getCarbonDataDirectoryPath(partitionId, segmentId);
    CarbonUtil.checkAndCreateFolder(carbonDataDirectoryPath);
    return carbonDataDirectoryPath;
  }

  /**
   * Comparator class for comparing 2 raw row result.
   */
  private class CarbonMdkeyComparator implements Comparator<RawResultIterator> {

    @Override public int compare(RawResultIterator o1, RawResultIterator o2) {

      Object[] row1 = new Object[0];
      Object[] row2 = new Object[0];
      try {
        row1 = o1.fetchConverted();
        row2 = o2.fetchConverted();
      } catch (KeyGenException e) {
        LOGGER.error(e.getMessage());
      }
      if (null == row1 || null == row2) {
        return 0;
      }
      ByteArrayWrapper key1 = (ByteArrayWrapper) row1[0];
      ByteArrayWrapper key2 = (ByteArrayWrapper) row2[0];
      int compareResult = 0;
      int[] columnValueSizes = segprop.getEachDimColumnValueSize();
      int dictionaryKeyOffset = 0;
      byte[] dimCols1 = key1.getDictionaryKey();
      byte[] dimCols2 = key2.getDictionaryKey();
      int noDicIndex = 0;
      for (int eachColumnValueSize : columnValueSizes) {
        // case of dictionary cols
        if (eachColumnValueSize > 0) {

          compareResult = ByteUtil.UnsafeComparer.INSTANCE
              .compareTo(dimCols1, dictionaryKeyOffset, eachColumnValueSize, dimCols2,
                  dictionaryKeyOffset, eachColumnValueSize);
          dictionaryKeyOffset += eachColumnValueSize;
        } else { // case of no dictionary

          byte[] noDictionaryDim1 = key1.getNoDictionaryKeyByIndex(noDicIndex);
          byte[] noDictionaryDim2 = key2.getNoDictionaryKeyByIndex(noDicIndex);
          compareResult =
              ByteUtil.UnsafeComparer.INSTANCE.compareTo(noDictionaryDim1, noDictionaryDim2);
          noDicIndex++;

        }
        if (0 != compareResult) {
          return compareResult;
        }
      }
      return 0;
    }
  }

}
