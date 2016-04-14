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

package org.carbondata.query.columnar.keyvalue;

import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreMetadata;
import org.carbondata.query.complex.querytypes.GenericQueryType;
import org.carbondata.query.wrappers.ByteArrayWrapper;

public class FilterScanResult extends AbstractColumnarScanResult {
    public FilterScanResult(int keySize, int[] selectedDimensionIndex) {
        super(keySize, selectedDimensionIndex);
    }

    public double getDoubleValue(int measureOrdinal) {
        return measureBlocks[measureOrdinal].getReadableDoubleValueByIndex(rowMapping[currentRow]);
    }

    public BigDecimal getBigDecimalValue(int measureOrdinal) {
        return measureBlocks[measureOrdinal]
                .getReadableBigDecimalValueByIndex(rowMapping[currentRow]);
    }

    public long getLongValue(int measureOrdinal) {
        return measureBlocks[measureOrdinal].getReadableLongValueByIndex(rowMapping[currentRow]);
    }

    public byte[] getByteArrayValue(int measureOrdinal) {
        return measureBlocks[measureOrdinal]
                .getReadableByteArrayValueByIndex(rowMapping[currentRow]);
    }

    public byte[] getKeyArray(ByteArrayWrapper key) {
        ++currentRow;
        return getKeyArray(rowMapping[++sourcePosition], key);
    }

    public List<byte[]> getKeyArrayWithComplexTypes(Map<Integer, GenericQueryType> complexQueryDims,
            ByteArrayWrapper keyVal) {
        ++currentRow;
        return getKeyArrayWithComplexTypes(rowMapping[++sourcePosition], complexQueryDims, keyVal);
    }

    @Override
    public int getDimDataForAgg(int dimOrdinal) {
        return getSurrogateKey(rowMapping[currentRow], dimOrdinal);
    }

    @Override
    public byte[] getKeyArray() {
        ++currentRow;
        return getKeyArray(rowMapping[++sourcePosition], null);
    }

    @Override
    public byte[] getNo_DictionayDimDataForAgg(int dimOrdinal) {
        ColumnarKeyStoreMetadata columnarKeyStoreMetadata =
                columnarKeyStoreDataHolder[dimOrdinal].getColumnarKeyStoreMetadata();
        List<byte[]> noDictionaryValsColumnarBlock=columnarKeyStoreDataHolder[dimOrdinal].getNoDictionaryValBasedKeyBlockData();
        if (null != noDictionaryValsColumnarBlock) {
            
            if (null == columnarKeyStoreMetadata.getColumnReverseIndex()) {
                return noDictionaryValsColumnarBlock.get(rowMapping[currentRow]);
            }
            return noDictionaryValsColumnarBlock
                    .get(columnarKeyStoreMetadata.getColumnReverseIndex()[rowMapping[currentRow]]);
        }
        return null;

    }

    @Override
    public void getComplexDimDataForAgg(GenericQueryType complexType,
            DataOutputStream dataOutputStream) throws IOException {
        getComplexSurrogateKey(rowMapping[currentRow], complexType, dataOutputStream);
    }

    public int getRowIndex() {
        return rowMapping[currentRow];
    }
}
