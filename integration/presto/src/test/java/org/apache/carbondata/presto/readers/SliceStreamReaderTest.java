/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.carbondata.presto.readers;

import com.facebook.presto.spi.block.SliceArrayBlock;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.slice.Slice;
import mockit.Mock;
import mockit.MockUp;
import org.apache.carbondata.core.cache.dictionary.AbstractColumnDictionaryInfo;
import org.apache.carbondata.core.cache.dictionary.DictionaryChunksWrapper;
import org.apache.carbondata.presto.CarbondataColumnHandle;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.StructField;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static junit.framework.TestCase.assertNotNull;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class SliceStreamReaderTest {
    private static SliceStreamReader sliceStreamReader;
    private static ColumnVector columnVector;
    private static Type type;
    private static CarbondataColumnHandle carbondataColumnHandle;
    private static Slice dictionarySlice[];

    @BeforeClass
    public static void setUp() {
        dictionarySlice=new Slice[2];
        dictionarySlice[0] = utf8Slice("dictSlice");
        dictionarySlice[1] = utf8Slice("dictSlice2");
    }

    @Test
    public void testReadBlockWithVectorReaderAndDict() throws IOException {
        StructField structField = new StructField("column1", IntegerType, false, null);
        columnVector = ColumnVector.allocate(5, structField.dataType(), MemoryMode.ON_HEAP);
        columnVector.putInt(1, 1);
        columnVector.putInt(2, 2);
        columnVector.putInt(3, 3);

        CarbondataColumnHandle carbondataColumnHandle = new CarbondataColumnHandle("connectorId", "id",
                com.facebook.presto.spi.type.IntegerType.INTEGER, 0, 3, 1, true, 1, "int", true, 5, 4);
        type = carbondataColumnHandle.getColumnType();
        new MockUp<AbstractColumnDictionaryInfo>() {
            @Mock
            public DictionaryChunksWrapper getDictionaryChunks() {
                byte[] dictChunks = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9, 1};
                List<byte[]> bytes = new ArrayList(Arrays.asList(dictChunks));
                return new DictionaryChunksWrapper(new ArrayList(Arrays.asList(bytes)));
            }
        };
        sliceStreamReader = new SliceStreamReader(true, new SliceArrayBlock(dictionarySlice.length, dictionarySlice));
        sliceStreamReader.setBatchSize(5);

        sliceStreamReader.setVector(columnVector);

        sliceStreamReader.setVectorReader(true);
        sliceStreamReader.readBlock(type);
        assertNotNull(sliceStreamReader);
    }

    @Test
    public void testReadBlockWithOutVectorReaderAndWithDict() throws IOException {
        StructField structField = new StructField("column1", StringType, false, null);
        columnVector = ColumnVector.allocate(5, structField.dataType(), MemoryMode.ON_HEAP);
        columnVector.putByteArray(1, "string1".getBytes());
        columnVector.putByteArray(2, "string2".getBytes());
        columnVector.putByteArray(3, "string3".getBytes());

        carbondataColumnHandle =
                new CarbondataColumnHandle("connectorId", "id", VarcharType.VARCHAR, 0, 3, 1, true, 1,
                        "string", true, 5, 4);
        type = carbondataColumnHandle.getColumnType();
        new MockUp<AbstractColumnDictionaryInfo>() {
            @Mock
            public DictionaryChunksWrapper getDictionaryChunks() {
                byte[] dictChunks = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9, 1};
                List<byte[]> bytes = new ArrayList(Arrays.asList(dictChunks));
                return new DictionaryChunksWrapper(new ArrayList(Arrays.asList(bytes)));
            }
        };
        sliceStreamReader = new SliceStreamReader(true, new SliceArrayBlock(dictionarySlice.length, dictionarySlice));
        sliceStreamReader.setBatchSize(5);

        sliceStreamReader.setVector(columnVector);

        sliceStreamReader.setVectorReader(false);
        sliceStreamReader.setStreamData(new Object[]{"1,2"});
        sliceStreamReader.readBlock(type);
        assertNotNull(sliceStreamReader);
    }

    @Test
    public void testReadBlockWithOutVectorReaderAndWithOutDict() throws IOException {
        StructField structField = new StructField("column1", StringType, false, null);
        columnVector = ColumnVector.allocate(5, structField.dataType(), MemoryMode.ON_HEAP);
        columnVector.putByteArray(1, "string1".getBytes());
        columnVector.putByteArray(2, "string2".getBytes());
        columnVector.putByteArray(3, "string3".getBytes());

        carbondataColumnHandle =
                new CarbondataColumnHandle("connectorId", "id", VarcharType.VARCHAR, 0, 3, 1, true, 1,
                        "string", true, 5, 4);
        type = carbondataColumnHandle.getColumnType();
        new MockUp<AbstractColumnDictionaryInfo>() {
            @Mock
            public DictionaryChunksWrapper getDictionaryChunks() {
                byte[] dictChunks = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9, 1};
                List<byte[]> bytes = new ArrayList(Arrays.asList(dictChunks));
                return new DictionaryChunksWrapper(new ArrayList(Arrays.asList(bytes)));
            }
        };
        sliceStreamReader = new SliceStreamReader(false,null);
        sliceStreamReader.setBatchSize(5);

        sliceStreamReader.setVector(columnVector);

        sliceStreamReader.setVectorReader(true);
        sliceStreamReader.setStreamData(new Object[]{"1,2,3,4"});
        sliceStreamReader.readBlock(type);
        assertNotNull(sliceStreamReader);
    }
}
