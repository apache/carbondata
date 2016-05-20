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

package org.carbondata.processing.merger.step.autoaggregate;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Arrays;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.util.CarbonUtilException;
import org.carbondata.processing.dimension.load.command.impl.DimenionLoadCommandHelper;
import org.carbondata.processing.merger.exeception.SliceMergerException;
import org.carbondata.processing.merger.util.CarbonSliceMergerUtil;
import org.carbondata.processing.util.CarbonDataProcessorUtil;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

public class CarbonAutoAggregateSliceMergerStep extends BaseStep {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonAutoAggregateSliceMergerStep.class.getName());
  /**
   * carbon data writer step data class
   */
  private CarbonAutoAggregateSliceMergerData data;

  /**
   * carbon data writer step meta
   */
  private CarbonAutoAggregateSliceMergerMeta meta;

  /**
   * readCounter
   */
  private long readCounter;

  /**
   * writeCounter
   */
  private long writeCounter;

  /**
   * isInitialise
   */
  private boolean isInitialise;

  /**
   * CarbonSliceMergerStep Constructor
   *
   * @param stepMeta          stepMeta
   * @param stepDataInterface stepDataInterface
   * @param copyNr            copyNr
   * @param transMeta         transMeta
   * @param trans             trans
   */
  public CarbonAutoAggregateSliceMergerStep(StepMeta stepMeta, StepDataInterface stepDataInterface,
      int copyNr, TransMeta transMeta, Trans trans) {
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
      meta = (CarbonAutoAggregateSliceMergerMeta) smi;

      // carbon data writer step data
      data = (CarbonAutoAggregateSliceMergerData) sdi;

      // get row from previous step, blocks when needed!
      Object[] row = getRow();
      // if row is null then there is no more incoming data
      if (null == row) {
        if (!isInitialise) {
          meta.initialise();
        }
        renameFolders();

        LOGGER.info("Record Procerssed For Auto Aggregation");
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
        meta.initialise();
        isInitialise = true;
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
      String[] tableNames = meta.getTableNames();
      boolean isDimFileRenameRequired = false;
      for (int i = 0; i < tableNames.length; i++) {
        isDimFileRenameRequired = (null == meta.getMapOfAggTableAndAgg().get(tableNames[i]));
        renameLoadFolderFromInProgressToNormal(
            meta.getSchemaName() + File.separator + meta.getCubeName(), tableNames[i],
            isDimFileRenameRequired);
        CarbonDataProcessorUtil.renameBadRecordsFromInProgressToNormal(
            meta.getSchemaName() + File.separator + meta.getCubeName());
      }

    } catch (SliceMergerException e) {
      throw new KettleException(e);
    }
  }

  private void deleteCheckPointFiles(final String tableName) {
    String tempLocationKey = meta.getSchemaName() + '_' + meta.getCubeName();
    String sortTmpFolderLocation = CarbonProperties.getInstance()
        .getProperty(tempLocationKey, CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL)
        + File.separator + meta.getSchemaName() + File.separator + meta.getCubeName();

    sortTmpFolderLocation =
        sortTmpFolderLocation + File.separator + CarbonCommonConstants.SORT_TEMP_FILE_LOCATION
            + File.separator + tableName;

    File sortTmpLocation = new File(sortTmpFolderLocation);
    File[] filesToDelete = sortTmpLocation.listFiles(new FileFilter() {

      @Override public boolean accept(File pathname) {
        if (pathname.getName().indexOf(tableName + CarbonCommonConstants.CHECKPOINT_EXT) > -1 ||
            pathname.getName().indexOf(tableName + CarbonCommonConstants.MEASUREMETADATA_FILE_EXT)
                > -1 || pathname.getName().startsWith(tableName)) {
          return true;
        }

        return false;

      }
    });

    try {
      CarbonUtil.deleteFiles(filesToDelete);
    } catch (CarbonUtilException e) {
      LOGGER.error("Unable to delete the checkpoints related files: "
          + Arrays.toString(filesToDelete));
    }

  }

  /**
   * @param storeLocation
   * @throws SliceMergerException
   */
  private boolean renameLoadFolderFromInProgressToNormal(String storeLocation, String tableName,
      boolean isDimensionAndHeirFileMergeRequired) throws SliceMergerException {
    // get the base store location
    String tempLocationKey = meta.getSchemaName() + '_' + meta.getCubeName();
    String baseStorelocation = CarbonProperties.getInstance()
        .getProperty(tempLocationKey, CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL)
        + File.separator + storeLocation;

    int rsCount = meta.getCurrentRestructNumber();
    if (rsCount < 0) {
      return false;
    }
    baseStorelocation =
        baseStorelocation + File.separator + CarbonCommonConstants.RESTRUCTRE_FOLDER + rsCount
            + File.separator + tableName;

    int counter = CarbonUtil.checkAndReturnCurrentLoadFolderNumber(baseStorelocation);
    if (counter < 0) {
      return false;
    }
    String destFol =
        baseStorelocation + File.separator + CarbonCommonConstants.LOAD_FOLDER + counter;

    baseStorelocation =
        baseStorelocation + File.separator + CarbonCommonConstants.LOAD_FOLDER + counter
            + CarbonCommonConstants.FILE_INPROGRESS_STATUS;

    // Merge the level files and hierarchy files before send the files
    // for final merge as the load is completed and now we are at the
    // step to rename the laod folder name.
    if (isDimensionAndHeirFileMergeRequired) {
      try {
        DimenionLoadCommandHelper.mergeFiles(baseStorelocation,
            CarbonSliceMergerUtil.getHeirAndKeySizeMap(meta.getHeirAndKeySize()));

      } catch (IOException e1) {
        LOGGER.error("Not able to merge the level Files");
        throw new SliceMergerException(e1.getMessage());
      }
    }
    File destFolder = new File(destFol);
    File currFolder = new File(baseStorelocation);
    if (!containsInProgressFiles(currFolder)) {
      if (!currFolder.renameTo(destFolder)) {
        throw new SliceMergerException("Problem while renaming inprogress folder to actual");
      }
    }
    File[] listFiles = destFolder.listFiles();
    if (null == listFiles || listFiles.length == 0) {
      try {
        CarbonUtil.deleteFoldersAndFiles(destFolder);
        return false;
      } catch (CarbonUtilException exception) {
        throw new SliceMergerException("Problem while deleting the empty load folder");
      }
    }
    return true;
  }

  private boolean containsInProgressFiles(File file) {
    File[] inProgressFiles = new File[0];
    file.listFiles(new FileFilter() {

      @Override public boolean accept(File fPath) {
        if (fPath.getName().endsWith(CarbonCommonConstants.FILE_INPROGRESS_STATUS)) {
          return true;
        }
        return false;
      }
    });

    if (inProgressFiles.length > 0) {
      return true;
    }
    return false;
  }

  /**
   * Initialize and do work where other steps need to wait for...
   *
   * @param smi The metadata to work with
   * @param sdi The data to initialize
   * @return step initialize or not
   */
  public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
    meta = (CarbonAutoAggregateSliceMergerMeta) smi;
    data = (CarbonAutoAggregateSliceMergerData) sdi;
    return super.init(smi, sdi);
  }

  /**
   * Dispose of this step: close files, empty logs, etc.
   *
   * @param smi The metadata to work with
   * @param sdi The data to dispose of
   */
  public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
    meta = (CarbonAutoAggregateSliceMergerMeta) smi;
    data = (CarbonAutoAggregateSliceMergerData) sdi;
    super.dispose(smi, sdi);
  }

}
