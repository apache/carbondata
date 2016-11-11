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

package org.apache.carbondata.core.carbon.datastore.chunk.impl;

import org.apache.carbondata.core.carbon.datastore.chunk.DimensionChunkAttributes;
import org.apache.carbondata.core.keygenerator.mdkey.MultiDimKeyVarLengthGenerator;
import org.apache.carbondata.scan.executor.infos.KeyStructureInfo;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

public class ColumnGroupDimensionDataChunkTest {

    static ColumnGroupDimensionDataChunk columnGroupDimensionDataChunk;
    static byte[] data;

    @BeforeClass
    public static void setup() {
        data = "dummy string".getBytes();
        DimensionChunkAttributes dimensionChunkAttributes = new DimensionChunkAttributes();
        dimensionChunkAttributes.setEachRowSize(4);

        int invertedIndex[] = {1, 3, 5, 7, 8};
        dimensionChunkAttributes.setInvertedIndexes(invertedIndex);
        columnGroupDimensionDataChunk = new ColumnGroupDimensionDataChunk(data, dimensionChunkAttributes);
    }

    @Test
    public void fillChunkDataTest() {
        KeyStructureInfo keyStructureInfo = new KeyStructureInfo();
        int[] maskByteRanges = {1, 2, 4, 6};
        keyStructureInfo.setMaskByteRanges(maskByteRanges);
        keyStructureInfo.setMaxKey("1234567".getBytes());

        int res = columnGroupDimensionDataChunk.fillChunkData(data, 2, 1, keyStructureInfo);
        assert (res == 4);
    }

    @Test
    public void getChunkDataTest() {
        byte expected[] = {121, 32, 115, 116};
        byte res[] = columnGroupDimensionDataChunk.getChunkData(1);
        assert (Arrays.equals(res, expected));
    }

    @Test
    public void fillConvertedChunkDataTest() {
        int[] row = {1, 2, 4, 6};
        int[] lens = {3, 5, 7, 9};
        int[] mdkeyQueryDimensionOrdinal = {0, 1, 2, 3};

        KeyStructureInfo keyStructureInfo = new KeyStructureInfo();
        keyStructureInfo.setKeyGenerator(new MultiDimKeyVarLengthGenerator(lens));
        keyStructureInfo.setMdkeyQueryDimensionOrdinal(mdkeyQueryDimensionOrdinal);

        int res = columnGroupDimensionDataChunk.fillConvertedChunkData(0, 0, row, keyStructureInfo);
        assert (res == 4);
    }
}

