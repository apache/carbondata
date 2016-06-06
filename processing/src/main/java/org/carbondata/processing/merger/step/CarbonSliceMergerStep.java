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

package org.carbondata.processing.merger.step;

import java.io.File;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.common.logging.impl.StandardLogService;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.path.CarbonStorePath;
import org.carbondata.core.carbon.path.CarbonTablePath;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.processing.merger.exeception.SliceMergerException;
import org.carbondata.processing.util.CarbonDataProcessorUtil;

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
      StandardLogService.setThreadName(StandardLogService.getPartitionID(meta.getCubeName()), null);
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

  /**
   * @throws KettleException
   */
  private void renameFolders() throws KettleException {
    try {
      // Rename the load Folder name as till part fact data should
      // beloaded properly
      // and renamed to normal.
      renameLoadFolderFromInProgressToNormal(
          meta.getSchemaName() + File.separator + meta.getCubeName());

      CarbonDataProcessorUtil.renameBadRecordsFromInProgressToNormal(
          meta.getSchemaName() + File.separator + meta.getCubeName());

    } catch (SliceMergerException e) {
      throw new KettleException(e);
    }
  }

  /**
   * @param storeLocation
   * @throws SliceMergerException
   */
  private boolean renameLoadFolderFromInProgressToNormal(String storeLocation)
      throws SliceMergerException {
    // get the base store location
    String tempLocationKey = meta.getSchemaName() + '_' + meta.getCubeName();
    String baseStorePath = CarbonProperties.getInstance()
        .getProperty(tempLocationKey, CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL);
    CarbonTableIdentifier carbonTableIdentifier =
        new CarbonTableIdentifier(meta.getSchemaName(), meta.getCubeName());
    CarbonTablePath carbonTablePath =
        CarbonStorePath.getCarbonTablePath(baseStorePath, carbonTableIdentifier);
    String partitionId = meta.getPartitionID();
    String carbonDataDirectoryPath = carbonTablePath.getCarbonDataDirectoryPath(partitionId,
        meta.getSegmentId()+"");
    carbonDataDirectoryPath =
        carbonDataDirectoryPath + File.separator + meta.getTaskNo();
    String baseStoreLocation =
        carbonDataDirectoryPath + CarbonCommonConstants.FILE_INPROGRESS_STATUS;
    File currentFolder = new File(baseStoreLocation);
    File destFolder = new File(carbonDataDirectoryPath);
    if (!currentFolder.renameTo(destFolder)) {
      throw new SliceMergerException("Problem while renaming inprogress folder to actual");
    }
    LOGGER.info("Folder renamed successfully to :: " + destFolder.getAbsolutePath());
    return true;
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
