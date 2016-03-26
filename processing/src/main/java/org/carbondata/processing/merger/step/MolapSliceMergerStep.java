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
import java.io.FileFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.common.logging.impl.StandardLogService;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.csvreader.checkpoint.CheckPointHanlder;
import org.carbondata.core.util.MolapProperties;
import org.carbondata.core.util.MolapUtil;
import org.carbondata.core.util.MolapUtilException;
import org.carbondata.processing.dimension.load.command.impl.DimenionLoadCommandHelper;
import org.carbondata.processing.merger.Util.MolapSliceMergerUtil;
import org.carbondata.processing.merger.exeception.SliceMergerException;
import org.carbondata.processing.util.LevelSortIndexWriter;
import org.carbondata.processing.util.MolapDataProcessorLogEvent;
import org.carbondata.processing.util.MolapDataProcessorUtil;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

public class MolapSliceMergerStep extends BaseStep {

    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(MolapSliceMergerStep.class.getName());
    /**
     * molap data writer step data class
     */
    private MolapSliceMergerStepData data;

    /**
     * molap data writer step meta
     */
    private MolapSliceMergerStepMeta meta;

    /**
     * readCounter
     */
    private long readCounter;

    /**
     * writeCounter
     */
    private long writeCounter;

    /**
     * MolapSliceMergerStep Constructor
     *
     * @param stepMeta          stepMeta
     * @param stepDataInterface stepDataInterface
     * @param copyNr            copyNr
     * @param transMeta         transMeta
     * @param trans             trans
     */
    public MolapSliceMergerStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
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
            // molap data writer step meta
            meta = (MolapSliceMergerStepMeta) smi;
            StandardLogService
                    .setThreadName(StandardLogService.getPartitionID(meta.getCubeName()), null);
            // molap data writer step data
            data = (MolapSliceMergerStepData) sdi;

            // get row from previous step, blocks when needed!
            Object[] row = getRow();
            // if row is null then there is no more incoming data
            if (null == row) {
                if (CheckPointHanlder.IS_CHECK_POINT_NEEDED && !meta.isGroupByEnabled()) {
                    if (!(getTrans().getErrors() > 0) && !(getTrans().isStopped())) {
                        renameFolders();
                    }
                } else {
                    renameFolders();
                }

                LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Record Procerssed For table: " + meta.getTabelName());
                String logMessage =
                        "Summary: Molap Slice Merger Step: Read: " + readCounter + ": Write: "
                                + writeCounter;
                LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, logMessage);
                //Delete the checkpoint and msrmetadata files from the sort
                //tmp folder as the processing is finished.
                if (CheckPointHanlder.IS_CHECK_POINT_NEEDED && !meta.isGroupByEnabled()) {
                    if (!(getTrans().getErrors() > 0) && !(getTrans().isStopped())) {
                        deleteCheckPointFiles();
                    }
                }
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
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, ex);
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

            MolapDataProcessorUtil.renameBadRecordsFromInProgressToNormal(
                    meta.getSchemaName() + File.separator + meta.getCubeName());

        } catch (SliceMergerException e) {
            throw new KettleException(e);
        }
    }

    private void deleteCheckPointFiles() {
        String tempLocationKey = meta.getSchemaName() + '_' + meta.getCubeName();
        String sortTmpFolderLoc = MolapProperties.getInstance()
                .getProperty(tempLocationKey, MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL)
                + File.separator + meta.getSchemaName() + File.separator + meta.getCubeName();

        sortTmpFolderLoc =
                sortTmpFolderLoc + File.separator + MolapCommonConstants.SORT_TEMP_FILE_LOCATION
                        + File.separator + meta.getTabelName();

        File sortTmpLocation = new File(sortTmpFolderLoc);
        File[] filesToDelete = sortTmpLocation.listFiles(new FileFilter() {

            @Override
            public boolean accept(File pathname) {
                if (pathname.getName()
                        .indexOf(meta.getTabelName() + MolapCommonConstants.CHECKPOINT_EXT) > -1 ||
                        pathname.getName().indexOf(
                                meta.getTabelName() + MolapCommonConstants.MEASUREMETADATA_FILE_EXT)
                                > -1 || pathname.getName().startsWith(meta.getTabelName())) {
                    return true;
                }

                return false;

            }
        });

        try {
            MolapUtil.deleteFiles(filesToDelete);
        } catch (MolapUtilException e) {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Unable to delete the checkpoints related files : " + Arrays
                            .toString(filesToDelete));
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
        String baseStorelocation = MolapProperties.getInstance()
                .getProperty(tempLocationKey, MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL)
                + File.separator + storeLocation;

        int rsCount = meta.getCurrentRestructNumber();
        if (rsCount < 0) {
            return false;
        }
        baseStorelocation =
                baseStorelocation + File.separator + MolapCommonConstants.RESTRUCTRE_FOLDER
                        + rsCount + File.separator + meta.getTabelName();

        int counter = MolapUtil.checkAndReturnCurrentLoadFolderNumber(baseStorelocation);
        if (counter < 0) {
            return false;
        }
        String destFol =
                baseStorelocation + File.separator + MolapCommonConstants.LOAD_FOLDER + counter;

        baseStorelocation =
                baseStorelocation + File.separator + MolapCommonConstants.LOAD_FOLDER + counter
                        + MolapCommonConstants.FILE_INPROGRESS_STATUS;

        //With Checkpoint case can be there when while loading the data to memory. 
        // It throws Outofmemory exception. and because of that it got restared , so in that case the in progress
        // folder will be renamed to normal folders. So In that case we need to return false here if 
        // the inprogress folder is not present.

        if (CheckPointHanlder.IS_CHECK_POINT_NEEDED && !meta.isGroupByEnabled()) {
            if (!(new File(baseStorelocation)).exists()) {
                return true;
            }
        }

        // Merge the level files and hierarchy files before send the files
        // for final merge as the load is completed and now we are at the
        // step to rename the laod folder name.
        if (!meta.isGroupByEnabled()) {
            try {
                DimenionLoadCommandHelper.mergeFiles(baseStorelocation,
                        MolapSliceMergerUtil.getHeirAndKeySizeMap(meta.getHeirAndKeySize()));

            } catch (IOException e1) {
                LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Not able to merge the level Files");
                throw new SliceMergerException(e1.getMessage());
            }

            String levelAnddataTypeString = meta.getLevelAnddataTypeString();
            String[] split = levelAnddataTypeString.split(MolapCommonConstants.HASH_SPC_CHARACTER);
            Map<String, String> levelAndDataTypeMap = new HashMap<String, String>();
            for (int i = 0; i < split.length; i++) {
                String[] split2 = split[i].split(MolapCommonConstants.COLON_SPC_CHARACTER);
                levelAndDataTypeMap.put(split2[0], split2[1]);
            }

            LevelSortIndexWriter levelFileUpdater = new LevelSortIndexWriter(levelAndDataTypeMap);
            levelFileUpdater.updateLevelFiles(baseStorelocation);

        }
        File currentFolder = new File(baseStorelocation);
        File destFolder = new File(destFol);
        if (!containsInProgressFiles(currentFolder)) {
            if (!currentFolder.renameTo(destFolder)) {
                throw new SliceMergerException(
                        "Problem while renaming inprogress folder to actual");
            }
            LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Folder renamed successfully to :: " + destFolder.getAbsolutePath());
        }

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
        meta = (MolapSliceMergerStepMeta) smi;
        data = (MolapSliceMergerStepData) sdi;
        return super.init(smi, sdi);
    }

    private boolean containsInProgressFiles(File file) {
        File[] inProgressNewFiles = null;
        inProgressNewFiles = file.listFiles(new FileFilter() {

            @Override
            public boolean accept(File file1) {
                if (file1.getName().endsWith(MolapCommonConstants.FILE_INPROGRESS_STATUS)) {
                    return true;
                }
                return false;
            }
        });

        if (inProgressNewFiles.length > 0) {
            return true;
        }
        return false;
    }

    /**
     * Dispose of this step: close files, empty logs, etc.
     *
     * @param smi The metadata to work with
     * @param sdi The data to dispose of
     */
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (MolapSliceMergerStepMeta) smi;
        data = (MolapSliceMergerStepData) sdi;
        super.dispose(smi, sdi);
    }

}
