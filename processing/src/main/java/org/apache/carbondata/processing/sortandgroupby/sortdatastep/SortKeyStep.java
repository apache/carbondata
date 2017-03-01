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

package org.apache.carbondata.processing.sortandgroupby.sortdatastep;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.common.logging.impl.StandardLogService;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.processing.schema.metadata.SortObserver;
import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortDataRows;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortIntermediateFileMerger;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortParameters;
import org.apache.carbondata.processing.util.NonDictionaryUtil;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

public class SortKeyStep extends BaseStep {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(SortKeyStep.class.getName());

  /**
   * CarbonSortKeyAndGroupByStepData
   */
  private SortKeyStepData data;

  /**
   * CarbonSortKeyAndGroupByStepMeta
   */
  private SortKeyStepMeta meta;

  /**
   * carbonSortKeys
   */
  private SortDataRows sortDataRows;

  /**
   * intermediateFileMerger
   */
  private SortIntermediateFileMerger intermediateFileMerger;

  /**
   * rowCounter
   */
  private long readCounter;

  /**
   * writeCounter
   */
  private long writeCounter;

  /**
   * logCounter
   */
  private int logCounter;

  /**
   * observer
   */
  private SortObserver observer;

  /**
   * To determine whether the column is dictionary or not.
   */
  private boolean[] noDictionaryColMaping;

  /**
   * CarbonSortKeyAndGroupByStep Constructor
   *
   * @param stepMeta
   * @param stepDataInterface
   * @param copyNr
   * @param transMeta
   * @param trans
   */
  public SortKeyStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
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
    // get step meta
    this.meta = ((SortKeyStepMeta) smi);
    StandardLogService.setThreadName(meta.getPartitionID(), null);
    // get step data
    this.data = ((SortKeyStepData) sdi);

    // get row
    Object[] row = getRow();

    // create sort observer
    this.observer = new SortObserver();

    // if row is null then this step can start processing the data
    if (row == null) {
      return processRowToNextStep();
    }

    // check if all records are null than send empty row to next step
    else if (NonDictionaryUtil.checkAllValuesForNull(row)) {
      // create empty row out size
      int outSize = Integer.parseInt(meta.getOutputRowSize());

      Object[] outRow = new Object[outSize];

      // clone out row meta
      this.data.setOutputRowMeta((RowMetaInterface) getInputRowMeta().clone());

      // get all fields
      this.meta.getFields(data.getOutputRowMeta(), getStepname(), null, null, this);

      LOGGER.info("Record Procerssed For table: " + meta.getTabelName());
      LOGGER.info("Record Form Previous Step was null");
      String logMessage = "Summary: Carbon Sort Key Step: Read: " + 1 + ": Write: " + 1;
      LOGGER.info(logMessage);

      putRow(data.getOutputRowMeta(), outRow);
      setOutputDone();
      return false;
    }

    // if first
    if (first) {
      CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordSortRowsStepTotalTime(
          meta.getPartitionID(), System.currentTimeMillis());
      first = false;

      // clone out row meta
      this.data.setOutputRowMeta((RowMetaInterface) getInputRowMeta().clone());

      // get all fields
      this.meta.getFields(data.getOutputRowMeta(), getStepname(), null, null, this);

      this.meta.setNoDictionaryCount(
          NonDictionaryUtil.extractNoDictionaryCount(meta.getNoDictionaryDims()));

      this.noDictionaryColMaping =
          NonDictionaryUtil.convertStringToBooleanArr(meta.getNoDictionaryDimsMapping());
      SortParameters parameters =
          SortParameters.createSortParameters(meta.getDatabaseName(), meta.getTabelName(),
              meta.getDimensionCount(), meta.getComplexDimensionCount(), meta.getMeasureCount(),
              meta.getNoDictionaryCount(), meta.getPartitionID(),
              meta.getSegmentId() + "", meta.getTaskNo(), this.noDictionaryColMaping);
      intermediateFileMerger = new SortIntermediateFileMerger(parameters);
      this.sortDataRows = new SortDataRows(parameters, intermediateFileMerger);
      try {
        // initialize sort
        this.sortDataRows.initialize();
      } catch (CarbonSortKeyAndGroupByException e) {
        throw new KettleException(e);
      }

      this.logCounter = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.DATA_LOAD_LOG_COUNTER,
              CarbonCommonConstants.DATA_LOAD_LOG_COUNTER_DEFAULT_COUNTER));
    }

    readCounter++;
    if (readCounter % logCounter == 0) {
      LOGGER.info("Record Procerssed For table: " + meta.getTabelName());
      String logMessage = "Carbon Sort Key Step: Record Read: " + readCounter;
      LOGGER.info(logMessage);
    }

    try {
      // add row
      this.sortDataRows.addRow(row);
      writeCounter++;
    } catch (Throwable e) {
      LOGGER.error(e);
      throw new KettleException(e);
    }

    return true;
  }

  /**
   * Below method will be used to process data to next step
   *
   * @return false is finished
   * @throws KettleException
   */
  private boolean processRowToNextStep() throws KettleException {
    if (null == this.sortDataRows) {
      LOGGER.info("Record Processed For table: " + meta.getTabelName());
      LOGGER.info("Number of Records was Zero");
      String logMessage = "Summary: Carbon Sort Key Step: Read: " + 0 + ": Write: " + 0;
      LOGGER.info(logMessage);
      putRow(data.getOutputRowMeta(), new Object[0]);
      setOutputDone();
      return false;
    }

    try {
      // start sorting
      this.sortDataRows.startSorting();
      this.intermediateFileMerger.finish();

      // check any more rows are present
      LOGGER.info("Record Processed For table: " + meta.getTabelName());
      String logMessage =
          "Summary: Carbon Sort Key Step: Read: " + readCounter + ": Write: " + writeCounter;
      LOGGER.info(logMessage);
      putRow(data.getOutputRowMeta(), new Object[0]);
      setOutputDone();
      CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordSortRowsStepTotalTime(
          meta.getPartitionID(), System.currentTimeMillis());
      CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordDictionaryValuesTotalTime(
          meta.getPartitionID(), System.currentTimeMillis());
      return false;
    } catch (CarbonSortKeyAndGroupByException e) {
      throw new KettleException(e);
    }

  }

  /**
   * Initialize and do work where other steps need to wait for...
   *
   * @param smi The metadata to work with
   * @param sdi The data to initialize
   * @return step initialize or not
   */
  public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
    this.meta = ((SortKeyStepMeta) smi);
    this.data = ((SortKeyStepData) sdi);
    return super.init(smi, sdi);
  }

  /**
   * Dispose of this step: close files, empty logs, etc.
   *
   * @param smi The metadata to work with
   * @param sdi The data to dispose of
   */
  public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
    this.meta = ((SortKeyStepMeta) smi);
    this.data = ((SortKeyStepData) sdi);
    this.sortDataRows = null;
    super.dispose(smi, sdi);
    this.meta = null;
    this.data = null;
  }
}