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

import java.io.File;
import java.nio.ByteBuffer;

public class MolapUnComressedSortTempFileReader extends
        AbstractSortTempFileReader
{

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
    public MolapUnComressedSortTempFileReader(int measureCount,
            int mdKeyLenght, boolean isFactMdkeyInSort, int factMdkeyLength,
            File tempFile, char[] type)
    {
        super(measureCount, mdKeyLenght, isFactMdkeyInSort, factMdkeyLength,
                tempFile, type);
    }

    /**
     * below method will be used to get chunk of rows
     * 
     * @return row
     */
    @Override
    public Object[][] getRow()
    {
        int recordSize = fileHolder.readInt(filePath);
        int byteArrayLength = fileHolder.readInt(filePath);
        byte[] readByteArray = fileHolder.readByteArray(filePath,
                byteArrayLength);
        return prepareRecordFromByteBuffer(recordSize, readByteArray);

    }

    private Object[][] prepareRecordFromByteBuffer(int recordSize, byte[] readByteArray)
    {
        Object[][] records = new Object[recordSize][];
        Object[] record = null;
        ByteBuffer buffer = ByteBuffer.allocate(readByteArray.length);
        buffer.put(readByteArray);
        buffer.rewind();
        int index = 0;
        byte[] byteArray = null;
        int length = 0;
        byte b = 0;
        for(int i = 0;i < recordSize;i++)
        {
            record = new Object[eachRecordSize];
            index = 0;
            for(int j = 0;j < measureCount;j++)
            {
                if(type[j] != 'c')
                {
                    b = buffer.get();
                    if(b == 1)
                    {
                        record[index++] = buffer.getDouble();
                    }
                    else
                    {
                        record[index++]= null;
                    }
                }
                else
                {
                    length = buffer.getInt();
                    byteArray = new byte[length];
                    buffer.get(byteArray);
                    record[index++] = byteArray;
                }
            }
            byteArray = new byte[mdKeyLenght];
            buffer.get(byteArray);
            record[index++] = byteArray;

            if(isFactMdkeyInSort)
            {
                byteArray = new byte[factMdkeyLength];
                buffer.get(byteArray);
                record[index++] = byteArray;
            }
            records[i] = record;
        }
        return records;
    }
}
