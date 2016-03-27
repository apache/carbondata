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

package org.carbondata.processing.csvreader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.vfs.FileObject;
import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.processing.util.CarbonDataProcessorLogEvent;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.csvinput.CsvInput;

public class CsvReader extends CsvInput {

    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(CsvReader.class.getName());

    /**
     * CsvReaderMeta
     */
    private CsvReaderMeta meta;

    /**
     * CsvReaderData
     */
    private CsvReaderData data;

    /**
     * bytesAlreadyRead
     */
    private long bytesAlreadyRead;

    /**
     * CsvReader Constructor
     *
     * @param stepMeta
     * @param stepDataInterface
     * @param copyNr
     * @param transMeta
     * @param trans
     */
    public CsvReader(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
            TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    /**
     * below method will be used to initialise file reader
     */
    @Override
    protected void initializeFileReader(FileObject fileObject) throws FileNotFoundException {
        super.initializeFileReader(fileObject);
        Map<String, Long> fileNameOffSetCache = meta.getFileNameOffSetCache();
        Long offset = fileNameOffSetCache.get(KettleVFS.getFilename(fileObject));
        if (null != offset) {
            bytesAlreadyRead = offset;
        } else {
            bytesAlreadyRead = 0;
        }
    }

    /**
     * Below method will be used to initialize the step
     */
    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (CsvReaderMeta) smi;
        data = (CsvReaderData) sdi;
        if (super.init(smi, sdi)) {
            if (0 == getCopy()) {
                meta.initializeCheckPoint(new File(getTrans().getFilename()).getName());

            }
            return true;
        }
        return false;
    }

    /**
     * Below method will be used to add the check point details to row
     */
    @Override
    protected void addRowDetails(Object[] outputRowData) {
        if (data.isAddingRowNumber) {
            outputRowData[data.rownumFieldIndex] = data.totalBytesFilesFinished + data.startBuffer;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                    "### " + data.totalBytesRead);
        }
    }

    /**
     * Below method will be used to open next file
     */
    @Override
    protected boolean openNextFile() throws KettleException {
        data.totalBytesFilesFinished = 0;
        return super.openNextFile();
    }

    /**
     * Below method will be used to read the header
     */
    public void readHeader() throws KettleException {
        if (0 == bytesAlreadyRead) {
            super.readHeader();
            return;
        }
        try {
            long skip = data.bufferedInputStream.skip(bytesAlreadyRead);
            if (skip > 0) {
                LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                        "Skipped bytes :" + skip);
            }
            data.totalBytesFilesFinished = bytesAlreadyRead;
        } catch (IOException e) {
            LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, e);
        }
    }

    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {

        if (null == smi && null == sdi) {
            return;
        }

        meta = null;
        super.dispose(smi, sdi);
    }

}
