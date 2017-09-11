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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.DecimalType;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructField;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;

import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.junit.Assert.assertNotNull;

public class DecimalSliceStreamReaderTest {

    private static DecimalSliceStreamReader decimalSliceStreamReader;
    private static ColumnVector columnVector;
    private static BigDecimal bigDecimal[];

    @Test
    public void readBlockTest() throws IOException {
        decimalSliceStreamReader = new DecimalSliceStreamReader();
        bigDecimal = new BigDecimal[2];
        bigDecimal[0] = new BigDecimal("22.12");
        bigDecimal[1] = new BigDecimal("99.25");
        decimalSliceStreamReader.setStreamData(bigDecimal);
        Block block = decimalSliceStreamReader.readBlock(DecimalType.createDecimalType());
        assertNotNull(block);
    }

    @Test
    public void readBlockTestWithDecimal() throws IOException {
        StructField structField = new StructField("column1", LongType, false, null);
        columnVector = ColumnVector.allocate(5, structField.dataType(), MemoryMode.ON_HEAP);
        decimalSliceStreamReader = new DecimalSliceStreamReader();
        decimalSliceStreamReader.setVector(columnVector);
        decimalSliceStreamReader.setVectorReader(true);
        decimalSliceStreamReader.setBatchSize(5);
        Decimal decimal = new Decimal();
        decimal.setOrNull(123654, 16, 2);
        columnVector.putDecimal(1, decimal, 10);
        Block block = decimalSliceStreamReader.readBlock(DecimalType.createDecimalType(16, 2));
        assertNotNull(block);
    }

    @Test
    public void readBlockTestWithLowActualPrecision() throws IOException {
        decimalSliceStreamReader = new DecimalSliceStreamReader();
        bigDecimal = new BigDecimal[]{new BigDecimal("99.527")};
        decimalSliceStreamReader.setStreamData(bigDecimal);
        Block block = decimalSliceStreamReader.readBlock(DecimalType.createDecimalType(20, 3));
        assertNotNull(block);
    }

}
