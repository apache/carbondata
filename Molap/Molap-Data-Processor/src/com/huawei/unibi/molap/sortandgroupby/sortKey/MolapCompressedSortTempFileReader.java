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

package com.huawei.unibi.molap.sortandgroupby.sortKey;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.datastorage.store.compression.SnappyCompression.SnappyByteCompression;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;
import com.huawei.unibi.molap.util.MolapUtil;

public class MolapCompressedSortTempFileReader extends AbstractSortTempFileReader {
    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(MolapCompressedSortTempFileReader.class.getName());

    /**
     * MolapCompressedSortTempFileReader
     *
     * @param measureCount
     * @param mdKeyLenght
     * @param isFactMdkeyInSort
     * @param factMdkeyLength
     * @param tempFile
     * @param type
     */
    public MolapCompressedSortTempFileReader(int measureCount, int mdKeyLenght,
            boolean isFactMdkeyInSort, int factMdkeyLength, File tempFile, char[] type) {
        super(measureCount, mdKeyLenght, isFactMdkeyInSort, factMdkeyLength, tempFile, type);
    }

    /**
     * below method will be used to get chunk of rows
     *
     * @return row
     */
    public Object[][] getRow() {

        ByteArrayInputStream[] byteArrayInputStream = new ByteArrayInputStream[measureCount];
        DataInputStream[] dataInputStream = new DataInputStream[measureCount];
        int recordSize = fileHolder.readInt(filePath);
        int readInt = 0;
        byte[] mdkey = null;
        byte[] factMdkey = null;
        try {
            for (int i = 0; i < measureCount; i++) {
                readInt = fileHolder.readInt(filePath);
                byteArrayInputStream[i] = new ByteArrayInputStream(SnappyByteCompression.INSTANCE
                        .unCompress(fileHolder.readByteArray(filePath, readInt)));
                dataInputStream[i] = new DataInputStream(byteArrayInputStream[i]);
            }
            readInt = fileHolder.readInt(filePath);
            mdkey = SnappyByteCompression.INSTANCE
                    .unCompress(fileHolder.readByteArray(filePath, readInt));
            factMdkey = null;
            if (isFactMdkeyInSort) {
                readInt = fileHolder.readInt(filePath);
                factMdkey = SnappyByteCompression.INSTANCE
                        .unCompress(fileHolder.readByteArray(filePath, readInt));
            }
        } finally {
            MolapUtil.closeStreams(byteArrayInputStream);
            MolapUtil.closeStreams(dataInputStream);
        }
        return prepareRecordFromStream(recordSize, dataInputStream, mdkey, factMdkey);
    }

    /**
     * Below method will be used to prepare the output record
     *
     * @param recordSize
     * @param measureBuffer
     * @param completeMdkey
     * @param completeMdkeyFactMdkey
     * @return
     */
    private Object[][] prepareRecordFromStream(int recordSize, DataInputStream[] measureBuffer,
            byte[] completeMdkey, byte[] completeMdkeyFactMdkey) {
        Object[][] records = new Object[recordSize][];
        Object[] record = null;
        int index = 0;
        byte[] mdkey = null;
        int mdkeyPosition = 0;
        int factMdkeyPosition = 0;
        int byteArraySize = 0;
        try {
            for (int i = 0; i < recordSize; i++) {
                record = new Object[eachRecordSize];
                index = 0;
                for (int j = 0; j < measureCount; j++) {
                    if (type[j] != 'c') {
                        if (measureBuffer[j].read() == 1) {
                            record[index++] = measureBuffer[j].readDouble();
                        } else {
                            record[index++] = null;
                        }
                    } else {
                        byteArraySize = measureBuffer[j].readInt();
                        mdkey = new byte[byteArraySize];
                        if (measureBuffer[j].read(mdkey) < 0) {
                            LOGGER.debug(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                                    "Problme while reading from stream");
                        }
                        record[index++] = mdkey;
                    }
                }
                mdkey = new byte[mdKeyLenght];
                System.arraycopy(completeMdkey, mdkeyPosition, mdkey, 0, mdKeyLenght);
                mdkeyPosition += mdKeyLenght;
                record[index++] = mdkey;
                if (isFactMdkeyInSort) {
                    mdkey = new byte[factMdkeyLength];
                    System.arraycopy(completeMdkeyFactMdkey, factMdkeyPosition, mdkey, 0,
                            factMdkeyLength);
                    factMdkeyPosition += factMdkeyLength;
                    record[index++] = mdkey;
                }
                records[i] = record;
            }
        } catch (IOException e) {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Problem while preparing output record ", e);
        }
        return records;
    }
}
