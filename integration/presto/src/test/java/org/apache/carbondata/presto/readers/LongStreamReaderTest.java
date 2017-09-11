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

import java.io.IOException;

import org.apache.carbondata.presto.CarbondataColumnHandle;

import com.facebook.presto.spi.type.BigintType;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.StructField;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertNotNull;
import static org.apache.spark.sql.types.DataTypes.LongType;

public class LongStreamReaderTest {
  private static LongStreamReader longStreamReader;
  private static CarbondataColumnHandle carbondataColumnHandle;
  private static ColumnVector columnVector;

  @BeforeClass public static void setUp() {
    StructField structField = new StructField("column1", LongType, false, null);

    columnVector = ColumnVector.allocate(3, structField.dataType(), MemoryMode.ON_HEAP);
    columnVector.reserve(3);
    columnVector.putLong(1, 1L);
    columnVector.putLong(2, 2L);
    columnVector.putNull(0);

    longStreamReader = new LongStreamReader();
    longStreamReader.setVector(columnVector);
    longStreamReader.setStreamData(new Object[] { 1L, 2L, 3L });
    carbondataColumnHandle =
        new CarbondataColumnHandle("connectorId", "id", BigintType.BIGINT, 0, 3, 1, true, 1, "int",
            true, 5, 4);

  }

  @Test public void testReadBlock() throws IOException {
    longStreamReader.setVectorReader(true);
    longStreamReader.setBatchSize(3);
    assertNotNull(longStreamReader.readBlock(carbondataColumnHandle.getColumnType()));
    longStreamReader.setVectorReader(false);
    assertNotNull(longStreamReader.readBlock(carbondataColumnHandle.getColumnType()));

  }
}
