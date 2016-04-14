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

package org.carbondata.query.complex.querytypes;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.core.carbon.SqlStatement;
import org.carbondata.query.datastorage.InMemoryTable;
import org.carbondata.query.evaluators.BlockDataHolder;
import org.carbondata.query.util.DataTypeConverter;
import org.carbondata.query.util.QueryExecutorUtility;

public class PrimitiveQueryType implements GenericQueryType {

    private int index;

    private String name;
    private String parentname;

    private int keySize;

    private int blockIndex;

    private SqlStatement.Type dataType;

    public PrimitiveQueryType(String name, String parentname, int blockIndex,
            SqlStatement.Type dataType) {
        this.name = name;
        this.parentname = parentname;
        this.blockIndex = blockIndex;
        this.dataType = dataType;
    }

    @Override
    public void addChildren(GenericQueryType children) {

    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getParentname() {
        return parentname;
    }

    @Override
    public void setParentname(String parentname) {
        this.parentname = parentname;

    }

    @Override
    public void getAllPrimitiveChildren(List<GenericQueryType> primitiveChild) {

    }

    @Override
    public int getSurrogateIndex() {
        return index;
    }

    @Override
    public void setSurrogateIndex(int surrIndex) {
        index = surrIndex;
    }

    @Override
    public int getBlockIndex() {
        return blockIndex;
    }

    @Override
    public void setBlockIndex(int blockIndex) {
        this.blockIndex = blockIndex;
    }

    @Override
    public int getColsCount() {
        return 1;
    }

    @Override
    public void parseBlocksAndReturnComplexColumnByteArray(
            ColumnarKeyStoreDataHolder[] columnarKeyStoreDataHolder, int rowNumber,
            DataOutputStream dataOutputStream) throws IOException {
        byte[] currentVal =
                new byte[columnarKeyStoreDataHolder[blockIndex].getColumnarKeyStoreMetadata()
                        .getEachRowSize()];
        if (!columnarKeyStoreDataHolder[blockIndex].getColumnarKeyStoreMetadata().isSorted()) {
            System.arraycopy(columnarKeyStoreDataHolder[blockIndex].getKeyBlockData(),
                    columnarKeyStoreDataHolder[blockIndex].getColumnarKeyStoreMetadata()
                            .getColumnReverseIndex()[rowNumber]
                            * columnarKeyStoreDataHolder[blockIndex].getColumnarKeyStoreMetadata().
                            getEachRowSize(), currentVal, 0, columnarKeyStoreDataHolder[blockIndex].
                            getColumnarKeyStoreMetadata().getEachRowSize());
        } else {
            System.arraycopy(columnarKeyStoreDataHolder[blockIndex].getKeyBlockData(),
                    rowNumber * columnarKeyStoreDataHolder[blockIndex].getColumnarKeyStoreMetadata()
                            .getEachRowSize(), currentVal, 0,
                    columnarKeyStoreDataHolder[blockIndex].getColumnarKeyStoreMetadata()
                            .getEachRowSize());
        }
        dataOutputStream.write(currentVal);
    }

    @Override
    public void setKeySize(int[] keyBlockSize) {
        this.keySize = keyBlockSize[this.blockIndex];
    }

    @Override
    public Object getDataBasedOnDataTypeFromSurrogates(List<InMemoryTable> slices,
            ByteBuffer surrogateData, Dimension[] dimensions) {
        byte[] data = new byte[keySize];
        surrogateData.get(data);
        String memberData = QueryExecutorUtility
                .getMemberBySurrogateKey(dimensions[blockIndex], unsignedIntFromByteArray(data),
                        slices).toString();
        Object actualData = DataTypeConverter.getDataBasedOnDataType(
                memberData.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL) ? null : memberData,
                dimensions[blockIndex].getDataType());
        if (null != actualData
                && dimensions[blockIndex].getDataType() == SqlStatement.Type.STRING) {
            byte[] dataBytes = ((String) actualData).getBytes(Charset.defaultCharset());
            return UTF8String.fromBytes(dataBytes);
        }
        return actualData;
    }

    private int unsignedIntFromByteArray(byte[] bytes) {
        int res = 0;
        if (bytes == null) return res;

        for (int i = 0; i < bytes.length; i++) {
            res = (res * 10) + ((bytes[i] & 0xff));
        }
        return res;
    }

    @Override
    public void parseAndGetResultBytes(ByteBuffer complexData, DataOutputStream dataOutput)
            throws IOException {

        //	    dataOutput.write();
    }

    @Override
    public DataType getSchemaType() {
        switch (dataType) {
        case INT:
            return new IntegerType();
        case DOUBLE:
            return new DoubleType();
        case LONG:
            return new LongType();
        case BOOLEAN:
            return new BooleanType();
        case TIMESTAMP:
            return new TimestampType();
        default:
            return new StringType();
        }
    }

    @Override
    public int getKeyOrdinalForQuery() {
        return 0;
    }

    @Override
    public void setKeyOrdinalForQuery(int keyOrdinalForQuery) {
    }

    @Override
    public void fillRequiredBlockData(BlockDataHolder blockDataHolder) {
        if (null == blockDataHolder.getColumnarKeyStore()[blockIndex]) {
            blockDataHolder.getColumnarKeyStore()[blockIndex] = blockDataHolder.getLeafDataBlock()
                    .getColumnarKeyStore(blockDataHolder.getFileHolder(), blockIndex, false,null);
        } else {
            if (!blockDataHolder.getColumnarKeyStore()[blockIndex].getColumnarKeyStoreMetadata()
                    .isUnCompressed()) {
                blockDataHolder.getColumnarKeyStore()[blockIndex].unCompress();
            }
        }
    }
}
