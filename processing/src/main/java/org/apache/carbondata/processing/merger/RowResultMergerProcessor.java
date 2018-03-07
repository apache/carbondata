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
package org.apache.carbondata.processing.merger;

import java.io.IOException;
import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.datastore.row.WriteStepRowUtil;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.result.iterator.RawResultIterator;
import org.apache.carbondata.core.scan.wrappers.ByteArrayWrapper;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.processing.exception.SliceMergerException;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerColumnar;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;
import org.apache.carbondata.processing.store.CarbonFactHandler;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

/**
 * This is the Merger class responsible for the merging of the segments.
 */
public class RowResultMergerProcessor extends AbstractResultProcessor {

  private CarbonFactHandler dataHandler;
  private SegmentProperties segprop;
  private CarbonLoadModel loadModel;
  private PartitionSpec partitionSpec;
  /**
   * record holder heap
   */
  private AbstractQueue<RawResultIterator> recordHolderHeap;

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(RowResultMergerProcessor.class.getName());

  public RowResultMergerProcessor(String databaseName,
      String tableName, SegmentProperties segProp, String[] tempStoreLocation,
      CarbonLoadModel loadModel, CompactionType compactionType, PartitionSpec partitionSpec) {
    this.segprop = segProp;
    this.partitionSpec = partitionSpec;
    this.loadModel = loadModel;
    CarbonDataProcessorUtil.createLocations(tempStoreLocation);

    CarbonTable carbonTable = CarbonMetadata.getInstance().getCarbonTable(databaseName, tableName);
    String carbonStoreLocation;
    if (partitionSpec != null) {
      carbonStoreLocation =
          partitionSpec.getLocation().toString() + CarbonCommonConstants.FILE_SEPARATOR + loadModel
              .getFactTimeStamp() + ".tmp";
    } else {
      carbonStoreLocation = CarbonDataProcessorUtil.createCarbonStoreLocation(
          loadModel.getDatabaseName(), tableName, loadModel.getSegmentId());
    }
    CarbonFactDataHandlerModel carbonFactDataHandlerModel = CarbonFactDataHandlerModel
        .getCarbonFactDataHandlerModel(loadModel, carbonTable, segProp, tableName,
            tempStoreLocation, carbonStoreLocation);
    setDataFileAttributesInModel(loadModel, compactionType, carbonFactDataHandlerModel);
    carbonFactDataHandlerModel.setCompactionFlow(true);
    dataHandler = new CarbonFactDataHandlerColumnar(carbonFactDataHandlerModel);
  }

  private void initRecordHolderHeap(List<RawResultIterator> rawResultIteratorList) {
    // create the List of RawResultIterator.
    recordHolderHeap = new PriorityQueue<RawResultIterator>(rawResultIteratorList.size(),
        new RowResultMergerProcessor.CarbonMdkeyComparator());
  }

  /**
   * Merge function
   *
   */
  public boolean execute(List<RawResultIterator> resultIteratorList) {
    initRecordHolderHeap(resultIteratorList);
    boolean mergeStatus = false;
    int index = 0;
    boolean isDataPresent = false;
    try {

      // add all iterators to the queue
      for (RawResultIterator leaftTupleIterator : resultIteratorList) {
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
        if (partitionSpec != null) {
          SegmentFileStore.writeSegmentFile(loadModel.getTablePath(), loadModel.getTaskNo(),
              partitionSpec.getLocation().toString(), loadModel.getFactTimeStamp() + "",
              partitionSpec.getPartitions());
        }
      } catch (CarbonDataWriterException | IOException e) {
        LOGGER.error(e, "Exception in compaction merger");
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
    CarbonRow row = WriteStepRowUtil.fromMergerRow(carbonTuple, segprop);
    try {
      this.dataHandler.addDataToStore(row);
    } catch (CarbonDataWriterException e) {
      throw new SliceMergerException("Problem in merging the slice", e);
    }
  }

  /**
   * Comparator class for comparing 2 raw row result.
   */
  private class CarbonMdkeyComparator implements Comparator<RawResultIterator> {
    int[] columnValueSizes = segprop.getEachDimColumnValueSize();
    public CarbonMdkeyComparator() {
      initSortColumns();
    }

    private void initSortColumns() {
      int numberOfSortColumns = segprop.getNumberOfSortColumns();
      if (numberOfSortColumns != columnValueSizes.length) {
        int[] sortColumnValueSizes = new int[numberOfSortColumns];
        System.arraycopy(columnValueSizes, 0, sortColumnValueSizes, 0, numberOfSortColumns);
        this.columnValueSizes = sortColumnValueSizes;
      }
    }

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
