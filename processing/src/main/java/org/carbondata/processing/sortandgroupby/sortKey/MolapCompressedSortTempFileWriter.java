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

package org.carbondata.processing.sortandgroupby.sortKey;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.datastorage.store.compression.SnappyCompression.SnappyByteCompression;
import org.carbondata.core.util.MolapUtil;
import org.carbondata.processing.sortandgroupby.exception.MolapSortKeyAndGroupByException;
import org.carbondata.query.aggregator.MeasureAggregator;

public class MolapCompressedSortTempFileWriter extends AbstractMolapSortTempFileWriter {

    /**
     * MolapCompressedSortTempFileWriter
     *
     * @param measureCount
     * @param mdkeyIndex
     * @param mdKeyLength
     * @param isFactMdkeyInSort
     * @param factMdkeyLength
     * @param writeFileBufferSize
     */
    public MolapCompressedSortTempFileWriter(int measureCount, int mdkeyIndex, int mdKeyLength,
            boolean isFactMdkeyInSort, int factMdkeyLength, int writeFileBufferSize, char[] type) {
        super(measureCount, mdkeyIndex, mdKeyLength, isFactMdkeyInSort, factMdkeyLength,
                writeFileBufferSize, type);
    }

    /**
     * Below method will be used to write the sort temp file
     *
     * @param records
     */
    public void writeSortTempFile(Object[][] records) throws MolapSortKeyAndGroupByException {
        ByteArrayOutputStream[] blockDataArray = null;
        DataOutputStream[] dataOutputStream = null;
        byte[] completeMdkey = null;
        byte[] completeFactMdkey = null;
        Object[] row = null;
        try {
            blockDataArray = new ByteArrayOutputStream[measureCount];
            dataOutputStream = new DataOutputStream[measureCount];
            initializeMeasureBuffers(blockDataArray, dataOutputStream, records.length);
            completeMdkey = new byte[mdKeyLength * records.length];
            if (isFactMdkeyInSort) {
                completeFactMdkey = new byte[factMdkeyLength * records.length];
            }
            for (int i = 0; i < records.length; i++) {
                row = records[i];
                int aggregatorIndexInRowObject = 0;
                // get row from record holder list
                MeasureAggregator[] aggregator =
                        (MeasureAggregator[]) row[aggregatorIndexInRowObject];
                for (int j = 0; j < aggregator.length; j++) {
                    byte[] byteArray = aggregator[j].getByteArray();
                    stream.writeInt(byteArray.length);
                    stream.write(byteArray);
                }
                stream.writeDouble((Double) row[mdkeyIndex - 1]);
                System.arraycopy((byte[]) row[mdkeyIndex], 0, completeMdkey, i * mdKeyLength,
                        mdKeyLength);
                if (isFactMdkeyInSort) {
                    System.arraycopy((byte[]) row[row.length - 1], 0, completeFactMdkey,
                            i * factMdkeyLength, factMdkeyLength);
                }
            }

            writeCompressData(records.length, completeMdkey, completeFactMdkey, blockDataArray,
                    isFactMdkeyInSort, writeFileBufferSize);

        } catch (IOException e) {
            throw new MolapSortKeyAndGroupByException(e);
        } finally {
            MolapUtil.closeStreams(blockDataArray);
            MolapUtil.closeStreams(dataOutputStream);
        }
    }

    /**
     * Below method will be used to write the compress data to temp file
     *
     * @param recordSize
     * @param mdkey
     * @param factMdkey
     * @param isFactMdkeyInSort
     * @param writeFileBufferSize
     * @throws IOException
     */
    private void writeCompressData(int recordSize, byte[] mdkey, byte[] factMdkey,
            ByteArrayOutputStream[] blockDataArray, boolean isFactMdkeyInSort,
            int writeFileBufferSize) throws IOException {
        stream.writeInt(recordSize);
        byte[] byteArray = null;
        for (int i = 0; i < blockDataArray.length; i++) {
            byteArray = SnappyByteCompression.INSTANCE.compress(blockDataArray[i].toByteArray());
            stream.writeInt(byteArray.length);
            stream.write(byteArray);
        }
        byteArray = SnappyByteCompression.INSTANCE.compress(mdkey);
        stream.writeInt(byteArray.length);
        stream.write(byteArray);
        if (isFactMdkeyInSort) {
            byteArray = SnappyByteCompression.INSTANCE.compress(factMdkey);
            stream.writeInt(byteArray.length);
            stream.write(byteArray);
        }
    }

    private void initializeMeasureBuffers(ByteArrayOutputStream[] blockDataArray,
            DataOutputStream[] dataOutputStream, int recordSizePerLeaf) {
        for (int i = 0; i < blockDataArray.length; i++) {
            blockDataArray[i] = new ByteArrayOutputStream(
                    recordSizePerLeaf * MolapCommonConstants.DOUBLE_SIZE_IN_BYTE);
            dataOutputStream[i] = new DataOutputStream(blockDataArray[i]);
        }
    }
}
