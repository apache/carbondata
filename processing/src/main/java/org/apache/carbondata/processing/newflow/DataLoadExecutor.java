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

package org.apache.carbondata.processing.newflow;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.processing.model.CarbonLoadModel;
import org.apache.carbondata.processing.newflow.exception.BadRecordFoundException;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.surrogatekeysgenerator.csvbased.BadRecordsLogger;

/**
 * It executes the data load.
 */
public class DataLoadExecutor {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DataLoadExecutor.class.getName());

  public void execute(CarbonLoadModel loadModel, String storeLocation,
      CarbonIterator<Object[]>[] inputIterators) throws Exception {
    AbstractDataLoadProcessorStep loadProcessorStep = null;
    try {

      loadProcessorStep =
          new DataLoadProcessBuilder().build(loadModel, storeLocation, inputIterators);
      // 1. initialize
      loadProcessorStep.initialize();
      LOGGER.info("Data Loading is started for table " + loadModel.getTableName());
      // 2. execute the step
      loadProcessorStep.execute();
    } catch (CarbonDataLoadingException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error(e, "Data Loading failed for table " + loadModel.getTableName());
      throw new CarbonDataLoadingException(
          "Data Loading failed for table " + loadModel.getTableName(), e);
    } finally {
      if (loadProcessorStep != null) {
        // 3. Close the step
        loadProcessorStep.close();
      }
    }

    String key =
        new CarbonTableIdentifier(loadModel.getDatabaseName(), loadModel.getTableName(), null)
            .getBadRecordLoggerKey();
    if (null != BadRecordsLogger.hasBadRecord(key)) {
      LOGGER.error("Data Load is partcially success for table " + loadModel.getTableName());
      throw new BadRecordFoundException("Bad records found during load");
    } else {
      LOGGER.info("Data loading is successful for table "+loadModel.getTableName());
    }
  }
}
