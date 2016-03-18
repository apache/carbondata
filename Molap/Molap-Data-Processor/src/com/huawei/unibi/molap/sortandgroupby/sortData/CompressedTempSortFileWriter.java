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

package com.huawei.unibi.molap.sortandgroupby.sortData;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.compression.SnappyCompression.SnappyByteCompression;
import com.huawei.unibi.molap.sortandgroupby.exception.MolapSortKeyAndGroupByException;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * Project Name 	: Carbon 
 * Module Name 		: MOLAP Data Processor
 * Author 			: Suprith T 72079 
 * Created Date 	: 18-Aug-2015
 * FileName 		: CompressedTempSortFileWriter.java
 * Description 		: Class for writing the compressed sort temp file
 * Class Version 	: 1.0
 */
public class CompressedTempSortFileWriter extends AbstractTempSortFileWriter
{

    /**
     * CompressedTempSortFileWriter
     * @param writeBufferSize
     * @param dimensionCount
     * @param measureCount
     */
    public CompressedTempSortFileWriter(int dimensionCount, int complexDimensionCount,
    		int measureCount,int highCardinalityCount, int writeBufferSize)
    {
        super(dimensionCount, complexDimensionCount, measureCount,highCardinalityCount, writeBufferSize);
    }

    /**
     * Below method will be used to write the sort temp file
     * @param records
     */
    public void writeSortTempFile(Object[][] records)
            throws MolapSortKeyAndGroupByException
    {
    	DataOutputStream dataOutputStream = null;
        ByteArrayOutputStream blockDataArray = null;
//        Object[] row = null;
        int totalSize = 0;
        int recordSize = 0;
        try
        {
        	recordSize = (measureCount * MolapCommonConstants.DOUBLE_SIZE_IN_BYTE) + (dimensionCount * MolapCommonConstants.INT_SIZE_IN_BYTE);
            totalSize = records.length * recordSize;
            
            blockDataArray = new ByteArrayOutputStream(totalSize);
            dataOutputStream = new DataOutputStream(blockDataArray);
            
            UnCompressedTempSortFileWriter.writeDataOutputStream(records, dataOutputStream, measureCount, dimensionCount, highCardinalityCount, complexDimensionCount);
            
            /*for(int recordIndex = 0; recordIndex < records.length; recordIndex++)
            {
                row = records[recordIndex];
                int fieldIndex = 0;
                
                for(int counter = 0; counter < dimensionCount; counter++)
                {
                	dataOutputStream.writeInt((Integer)row[fieldIndex++]);
                }
                
                for(int counter = 0; counter < measureCount; counter++)
                {
                	if(null != row[fieldIndex])
                    {
                        dataOutputStream.write((byte)1);
                        dataOutputStream.writeDouble((Double)row[fieldIndex]);
                    }
                    else
                    {
                        dataOutputStream.write((byte)0);
                    }
                	
                	fieldIndex++;
                }
                
            }*/
            stream.writeInt(records.length);
            byte[] byteArray = SnappyByteCompression.INSTANCE
                    .compress(blockDataArray.toByteArray());
            stream.writeInt(byteArray.length);
            stream.write(byteArray);

        }
        catch(IOException e)
        {
            throw new MolapSortKeyAndGroupByException(e);
        }
        finally
        {
            MolapUtil.closeStreams(blockDataArray);
            MolapUtil.closeStreams(dataOutputStream);
        }
    }
}
