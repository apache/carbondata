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
package org.apache.carbondata.processing.partition.spliter;

import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.datastore.row.WriteStepRowUtil;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.partition.spliter.exception.AlterPartitionSliceException;
import org.apache.carbondata.processing.store.CarbonDataFileAttributes;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerColumnar;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;
import org.apache.carbondata.processing.store.CarbonFactHandler;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

public class RowResultProcessor {

  private CarbonFactHandler dataHandler;
  private SegmentProperties segmentProperties;

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(RowResultProcessor.class.getName());


  public RowResultProcessor(CarbonTable carbonTable, CarbonLoadModel loadModel,
      SegmentProperties segProp, String[] tempStoreLocation, Integer bucketId) {
    CarbonDataProcessorUtil.createLocations(tempStoreLocation);
    this.segmentProperties = segProp;
    String tableName = carbonTable.getTableName();
    String carbonStoreLocation = CarbonDataProcessorUtil.createCarbonStoreLocation(
        loadModel.getDatabaseName(), tableName, loadModel.getSegmentId());
    CarbonFactDataHandlerModel carbonFactDataHandlerModel =
        CarbonFactDataHandlerModel.getCarbonFactDataHandlerModel(loadModel, carbonTable,
            segProp, tableName, tempStoreLocation, carbonStoreLocation);
    CarbonDataFileAttributes carbonDataFileAttributes =
        new CarbonDataFileAttributes(Long.parseLong(loadModel.getTaskNo()),
            loadModel.getFactTimeStamp());
    carbonFactDataHandlerModel.setCarbonDataFileAttributes(carbonDataFileAttributes);
    carbonFactDataHandlerModel.setBucketId(bucketId);
    //Note: set compaction flow just to convert decimal type
    carbonFactDataHandlerModel.setCompactionFlow(true);
    dataHandler = new CarbonFactDataHandlerColumnar(carbonFactDataHandlerModel);
  }

  public boolean execute(List<Object[]> resultList) {
    boolean processStatus;
    boolean isDataPresent = false;

    try {
      if (!isDataPresent) {
        dataHandler.initialise();
        isDataPresent = true;
      }
      for (Object[] row: resultList) {
        addRow(row);
      }
      if (isDataPresent)
      {
        this.dataHandler.finish();
      }
      processStatus = true;
    } catch (AlterPartitionSliceException e) {
      LOGGER.error(e, e.getMessage());
      LOGGER.error("Exception in executing RowResultProcessor" + e.getMessage());
      processStatus = false;
    } finally {
      try {
        if (isDataPresent) {
          this.dataHandler.closeHandler();
        }
      } catch (Exception e) {
        LOGGER.error("Exception while closing the handler in RowResultProcessor" + e.getMessage());
        processStatus = false;
      }
    }
    return processStatus;
  }

  private void addRow(Object[] carbonTuple) throws AlterPartitionSliceException {
    CarbonRow row = WriteStepRowUtil.fromMergerRow(carbonTuple, segmentProperties);
    try {
      this.dataHandler.addDataToStore(row);
    } catch (CarbonDataWriterException e) {
      throw new AlterPartitionSliceException("Exception in adding rows in RowResultProcessor", e);
    }
  }
}
