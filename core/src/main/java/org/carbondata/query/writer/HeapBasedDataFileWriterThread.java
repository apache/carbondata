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

package org.carbondata.query.writer;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.AbstractQueue;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.util.MolapUtil;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.executer.Tuple;
import org.carbondata.query.schema.metadata.DataProcessorInfo;
import org.carbondata.query.util.MolapEngineLogEvent;

public class HeapBasedDataFileWriterThread extends ResultWriter {

    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(HeapBasedDataFileWriterThread.class.getName());
    /**
     * dataHeap
     */
    private AbstractQueue<Tuple> dataHeap;

    /***
     * comparator
     */
    // private Comparator comparator;
    /**
     * outLocation
     */
    private String outLocation;

    /**
     * DataFileWriter {refer Constructor}
     *
     * @param dataMap     data
     * @param outLocation out location
     * @param queryId     query id
     */
    public HeapBasedDataFileWriterThread(AbstractQueue<Tuple> dataHeap, DataProcessorInfo writerVo,
            String outLocation) {
        this.outLocation = outLocation;

        this.dataProcessorInfo = writerVo;

        this.dataHeap = dataHeap;

        //        updateDuplicateDimensions();

    }

    /**
     * @see java.util.concurrent.Callable#call()
     */
    @Override
    public Void call() throws Exception {
        DataOutputStream dataOutput = null;
        try {
            if (!new File(this.outLocation).mkdirs()) {
                LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                        "Problem while creating the pagination directory");
            }

            File tempFile =
                    new File(this.outLocation + File.separator + System.nanoTime() + ".tmp");

            dataOutput = FileFactory.getDataOutputStream(tempFile.getAbsolutePath(),
                    FileFactory.getFileType(tempFile.getAbsolutePath()),
                    MolapCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR
                            * MolapCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR);

            int size = this.dataHeap.size();

            dataOutput.writeInt(size);

            writeDataFromHeap(dataOutput);

            File dest = new File(this.outLocation + File.separator + System.nanoTime()
                    + MolapCommonConstants.QUERY_OUT_FILE_EXT);
            if (!tempFile.renameTo(dest)) {
                LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                        "Problem while renaming the file");
            }
        } finally {
            MolapUtil.closeStreams(dataOutput);
        }
        this.dataHeap = null;
        return null;
    }

    /**
     * Below method will be used to write data from heap to file
     *
     * @param dataOutput
     * @throws IOException
     * @throws KeyGenException
     */
    private void writeDataFromHeap(DataOutputStream dataOutput)
            throws IOException, KeyGenException {
        int size = dataHeap.size();
        for (int i = 0; i < size; i++) {
            Tuple poll = dataHeap.poll();
            byte[] key = poll.getKey();
            dataOutput.write(key);
            MeasureAggregator[] value = poll.getMeasures();
            for (int j = 0; j < value.length; j++) {
                value[j].writeData(dataOutput);
            }
        }
    }

}
