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

package org.apache.carbondata.processing.loading;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.processing.loading.exception.BadRecordFoundException;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.loading.exception.NoRetryException;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;

/**
 * It executes the data load.
 */
public class DataLoadExecutor {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DataLoadExecutor.class.getName());

  private AbstractDataLoadProcessorStep loadProcessorStep;

  private boolean isClosed;

  public void execute(CarbonLoadModel loadModel, String[] storeLocation,
      CarbonIterator<Object[]>[] inputIterators) throws Exception {
    try {
      loadProcessorStep =
          new DataLoadProcessBuilder().build(loadModel, storeLocation, inputIterators);
      // 1. initialize
      loadProcessorStep.initialize();
      LOGGER.info("Data Loading is started for table " + loadModel.getTableName());
      // 2. execute the step
      loadProcessorStep.execute();
      // check and remove any bad record key from bad record entry logger static map
      if (badRecordFound(
          loadModel.getCarbonDataLoadSchema().getCarbonTable().getCarbonTableIdentifier())) {
        LOGGER.error("Data Load is partially success for table " + loadModel.getTableName());
      } else {
        LOGGER.info("Data loading is successful for table " + loadModel.getTableName());
      }
    } catch (CarbonDataLoadingException e) {
      if (e instanceof BadRecordFoundException) {
        throw new NoRetryException(e.getMessage());
      } else {
        throw e;
      }
    } catch (Exception e) {
      LOGGER.error(e, "Data Loading failed for table " + loadModel.getTableName());
      throw new CarbonDataLoadingException(
          "Data Loading failed for table " + loadModel.getTableName(), e);
    } finally {
      removeBadRecordKey(
          loadModel.getCarbonDataLoadSchema().getCarbonTable().getCarbonTableIdentifier());
    }
  }

  /**
   * This method will remove any bad record key from the map entry
   *
   * @param carbonTableIdentifier
   * @return
   */
  private boolean badRecordFound(CarbonTableIdentifier carbonTableIdentifier) {
    String badRecordLoggerKey = carbonTableIdentifier.getBadRecordLoggerKey();
    boolean badRecordKeyFound = false;
    if (null != BadRecordsLogger.hasBadRecord(badRecordLoggerKey)) {
      badRecordKeyFound = true;
    }
    return badRecordKeyFound;
  }

  /**
   * This method will remove the bad record key from bad record logger
   *
   * @param carbonTableIdentifier
   */
  private void removeBadRecordKey(CarbonTableIdentifier carbonTableIdentifier) {
    String badRecordLoggerKey = carbonTableIdentifier.getBadRecordLoggerKey();
    BadRecordsLogger.removeBadRecordKey(badRecordLoggerKey);
  }

  /**
   * Method to clean all the resource
   */
  public void close() {
    if (!isClosed && loadProcessorStep != null) {
      loadProcessorStep.close();
    }
    isClosed = true;
  }
}
