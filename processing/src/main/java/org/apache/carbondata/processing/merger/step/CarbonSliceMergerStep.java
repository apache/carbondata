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

package org.apache.carbondata.processing.merger.step;

import java.io.File;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.common.logging.impl.StandardLogService;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

public class CarbonSliceMergerStep extends BaseStep {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonSliceMergerStep.class.getName());
  /**
   * carbon data writer step data class
   */
  private CarbonSliceMergerStepData data;

  /**
   * carbon data writer step meta
   */
  private CarbonSliceMergerStepMeta meta;

  /**
   * readCounter
   */
  private long readCounter;

  /**
   * writeCounter
   */
  private long writeCounter;

  /**
   * CarbonSliceMergerStep Constructor
   *
   * @param stepMeta          stepMeta
   * @param stepDataInterface stepDataInterface
   * @param copyNr            copyNr
   * @param transMeta         transMeta
   * @param trans             trans
   */
  public CarbonSliceMergerStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
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
      // carbon data writer step meta
      meta = (CarbonSliceMergerStepMeta) smi;
      StandardLogService.setThreadName(StandardLogService.getPartitionID(meta.getTableName()),
          null);
      // carbon data writer step data
      data = (CarbonSliceMergerStepData) sdi;

      // get row from previous step, blocks when needed!
      Object[] row = getRow();
      // if row is null then there is no more incoming data
      if (null == row) {
        renameFolders();

        LOGGER.info("Record Procerssed For table: " + meta.getTabelName());
        String logMessage =
            "Summary: Carbon Slice Merger Step: Read: " + readCounter + ": Write: " + writeCounter;
        LOGGER.info(logMessage);
        // step processing is finished
        setOutputDone();
        // return false
        return false;
      }

      if (first) {
        first = false;
        if (getInputRowMeta() != null) {
          this.data.setOutputRowMeta((RowMetaInterface) getInputRowMeta().clone());
          this.meta.getFields(data.getOutputRowMeta(), getStepname(), null, null, this);
        }
      }
      readCounter++;
    } catch (Exception ex) {
      LOGGER.error(ex);
      throw new RuntimeException(ex);
    }
    return true;
  }

  private void renameFolders() {
    CarbonDataProcessorUtil.renameBadRecordsFromInProgressToNormal(
        meta.getDatabaseName() + File.separator + meta.getTableName() + File.separator + meta
            .getTaskNo());
  }

  /**
   * Initialize and do work where other steps need to wait for...
   *
   * @param smi The metadata to work with
   * @param sdi The data to initialize
   * @return step initialize or not
   */
  public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
    meta = (CarbonSliceMergerStepMeta) smi;
    data = (CarbonSliceMergerStepData) sdi;
    return super.init(smi, sdi);
  }

  /**
   * Dispose of this step: close files, empty logs, etc.
   *
   * @param smi The metadata to work with
   * @param sdi The data to dispose of
   */
  public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
    meta = (CarbonSliceMergerStepMeta) smi;
    data = (CarbonSliceMergerStepData) sdi;
    super.dispose(smi, sdi);
  }

}
