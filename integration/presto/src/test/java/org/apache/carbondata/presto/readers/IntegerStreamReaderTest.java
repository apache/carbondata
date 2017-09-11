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

import com.facebook.presto.spi.block.Block;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.StructField;
import org.junit.Test;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.junit.Assert.assertNotNull;

public class IntegerStreamReaderTest {
  private static ColumnVector columnVector;
  private static IntegerStreamReader integerStreamReader;
  private int RANDOM_DATA = 4;

  @Test public void readBlockTest() throws IOException {
    integerStreamReader = new IntegerStreamReader();
    Object[] object = new Object[] { RANDOM_DATA };
    integerStreamReader.setStreamData(object);
    Block block = integerStreamReader.readBlock(com.facebook.presto.spi.type.IntegerType.INTEGER);
    assertNotNull(block);
  }

  @Test public void readVectorizedBlock() throws IOException {
    integerStreamReader = new IntegerStreamReader();
    StructField structField = new StructField("column1", IntegerType, false, null);
    columnVector = ColumnVector.allocate(5, structField.dataType(), MemoryMode.ON_HEAP);
    columnVector.putInt(2, 35);
    columnVector.putNull(3);
    integerStreamReader.setVector(columnVector);
    integerStreamReader.setVectorReader(true);
    integerStreamReader.setBatchSize(5);
    Block block = integerStreamReader.readBlock(com.facebook.presto.spi.type.IntegerType.INTEGER);
    assertNotNull(block);
  }

}
