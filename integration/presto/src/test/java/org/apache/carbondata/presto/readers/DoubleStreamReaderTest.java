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

import com.facebook.presto.spi.type.Type;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.StructField;
import org.junit.Test;

import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.junit.Assert.assertNotNull;

public class DoubleStreamReaderTest {
  private static Object[] objects;
  private static Type type = com.facebook.presto.spi.type.DoubleType.DOUBLE;
  private static DoubleStreamReader doubleStreamReader;
  private static ColumnVector columnVector;

  @Test public void testReadBlock() throws IOException {
    doubleStreamReader = new DoubleStreamReader();
    objects = new Object[] { (double) 1 };
    doubleStreamReader.setStreamData(objects);
    doubleStreamReader.readBlock(type);
    assertNotNull(doubleStreamReader);
  }

  @Test public void testVectorizedReadBlock() throws IOException {
    StructField structField = new StructField("column1", DoubleType, false, null);
    columnVector = ColumnVector.allocate(5, structField.dataType(), MemoryMode.ON_HEAP);
    columnVector.putDouble(0, 11);
    columnVector.putNull(2);
    doubleStreamReader = new DoubleStreamReader();
    doubleStreamReader.setVector(columnVector);
    doubleStreamReader.setVectorReader(true);
    doubleStreamReader.setBatchSize(5);
    doubleStreamReader.readBlock(type);
    assertNotNull(doubleStreamReader);
  }
}
