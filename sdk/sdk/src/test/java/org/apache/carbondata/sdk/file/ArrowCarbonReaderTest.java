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

package org.apache.carbondata.sdk.file;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.sdk.file.arrow.ArrowConverter;
import org.apache.carbondata.sdk.file.arrow.ArrowUtils;

import junit.framework.TestCase;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ArrowCarbonReaderTest extends TestCase {

  @Before
  public void cleanFile() {
    assert (TestUtil.cleanMdtFile());
  }

  @After
  public void verifyDMFile() {
    assert (!TestUtil.verifyMdtFile());
    String path = "./testWriteFiles";
    try {
      FileUtils.deleteDirectory(new File(path));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testArrowReader() {
    String path = "./carbondata";
    try {
      FileUtils.deleteDirectory(new File(path));

      Field[] fields = new Field[13];
      fields[0] = new Field("stringField", DataTypes.STRING);
      fields[1] = new Field("shortField", DataTypes.SHORT);
      fields[2] = new Field("intField", DataTypes.INT);
      fields[3] = new Field("longField", DataTypes.LONG);
      fields[4] = new Field("doubleField", DataTypes.DOUBLE);
      fields[5] = new Field("boolField", DataTypes.BOOLEAN);
      fields[6] = new Field("dateField", DataTypes.DATE);
      fields[7] = new Field("timeField", DataTypes.TIMESTAMP);
      fields[8] = new Field("decimalField", DataTypes.createDecimalType(8, 2));
      fields[9] = new Field("varcharField", DataTypes.VARCHAR);
      fields[10] = new Field("arrayField", DataTypes.createArrayType(DataTypes.STRING));
      fields[11] = new Field("floatField", DataTypes.FLOAT);
      fields[12] = new Field("binaryField", DataTypes.BINARY);
      Map<String, String> map = new HashMap<>();
      map.put("complex_delimiter_level_1", "#");
      CarbonWriter writer = CarbonWriter.builder()
          .outputPath(path)
          .withLoadOptions(map)
          .withCsvInput(new Schema(fields))
          .writtenBy("CarbonReaderTest")
          .build();
      byte[] value = "Binary".getBytes();
      for (int i = 0; i < 10; i++) {
        Object[] row2 = new Object[]{
            "robot" + (i % 10),
            i % 10000,
            i,
            (Long.MAX_VALUE - i),
            ((double) i / 2),
            (true),
            "2019-03-02",
            "2019-02-12 03:03:34",
            12.345,
            "varchar",
            "Hello#World#From#Carbon",
            1.23,
            value
        };
        writer.write(row2);
      }
      writer.close();
      // Read data
      ArrowCarbonReader reader =
          CarbonReader.builder(path, "_temp").withRowRecordReader().buildArrowReader();
      Schema carbonSchema = CarbonSchemaReader.readSchema(path);
      byte[] data = reader.readArrowBatch(carbonSchema);
      BufferAllocator bufferAllocator = ArrowUtils.rootAllocator.newChildAllocator("toArrowBuffer", 0, Long.MAX_VALUE);
      ArrowRecordBatch arrowRecordBatch =
          ArrowConverter.byteArrayToArrowBatch(data, bufferAllocator);
      VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot
          .create(ArrowUtils.toArrowSchema(carbonSchema, TimeZone.getDefault().getID()),
              bufferAllocator);
      VectorLoader vectorLoader = new VectorLoader(vectorSchemaRoot);
      vectorLoader.load(arrowRecordBatch);
      // check for 10 rows
      assertEquals(vectorSchemaRoot.getRowCount(), 10);
      List<FieldVector> fieldVectors = vectorSchemaRoot.getFieldVectors();
      // validate short column
      for (int i = 0; i < vectorSchemaRoot.getRowCount(); i++) {
        assertEquals(((SmallIntVector)fieldVectors.get(6)).get(i), i);
      }
      // validate float column
      for (int i = 0; i < vectorSchemaRoot.getRowCount(); i++) {
        assertEquals(((Float4Vector)fieldVectors.get(12)).get(i), (float) 1.23);
      }
      arrowRecordBatch.close();
      vectorSchemaRoot.close();
      bufferAllocator.close();
      reader.close();

      // Read data with address (unsafe memory)
      ArrowCarbonReader reader1 =
          CarbonReader.builder(path, "_temp").withRowRecordReader().buildArrowReader();
      long address = reader1.readArrowBatchAddress(carbonSchema);
      int length = CarbonUnsafe.getUnsafe().getInt(address);
      byte[] data1 = new byte[length];
      CarbonUnsafe.getUnsafe().copyMemory(null, address + 4 , data1, CarbonUnsafe.BYTE_ARRAY_OFFSET, length);
      bufferAllocator = ArrowUtils.rootAllocator.newChildAllocator("toArrowBuffer", 0, Long.MAX_VALUE);
      arrowRecordBatch =
          ArrowConverter.byteArrayToArrowBatch(data1, bufferAllocator);
      vectorSchemaRoot = VectorSchemaRoot
          .create(ArrowUtils.toArrowSchema(carbonSchema, TimeZone.getDefault().getID()),
              bufferAllocator);
      vectorLoader = new VectorLoader(vectorSchemaRoot);
      vectorLoader.load(arrowRecordBatch);
      // check for 10 rows
      assertEquals(vectorSchemaRoot.getRowCount(), 10);
      List<FieldVector> fieldVectors1 = vectorSchemaRoot.getFieldVectors();
      // validate short column
      for (int i = 0; i < vectorSchemaRoot.getRowCount(); i++) {
        assertEquals(((SmallIntVector)fieldVectors1.get(6)).get(i), i);
      }
      // validate float column
      for (int i = 0; i < vectorSchemaRoot.getRowCount(); i++) {
        assertEquals(((Float4Vector)fieldVectors1.get(12)).get(i), (float) 1.23);
      }
      arrowRecordBatch.close();
      vectorSchemaRoot.close();
      bufferAllocator.close();
      // free the unsafe memory
      reader1.freeArrowBatchMemory(address);
      reader1.close();


      // Read as arrow vector
      ArrowCarbonReader reader2 =
          CarbonReader.builder(path, "_temp").withRowRecordReader().buildArrowReader();
      VectorSchemaRoot vectorSchemaRoot2 = reader2.readArrowVectors(carbonSchema);
      // check for 10 rows
      assertEquals(vectorSchemaRoot2.getRowCount(), 10);
      List<FieldVector> fieldVectors2 = vectorSchemaRoot2.getFieldVectors();
      // validate short column
      for (int i = 0; i < vectorSchemaRoot2.getRowCount(); i++) {
        assertEquals(((SmallIntVector)fieldVectors2.get(6)).get(i), i);
      }
      // validate float column
      for (int i = 0; i < vectorSchemaRoot2.getRowCount(); i++) {
        assertEquals(((Float4Vector)fieldVectors2.get(12)).get(i), (float) 1.23);
      }
      vectorSchemaRoot.close();
      reader2.close();

      // Read arrowSchema
      byte[] schema = CarbonSchemaReader.getArrowSchemaAsBytes(path);
      bufferAllocator = ArrowUtils.rootAllocator.newChildAllocator("toArrowBuffer", 0, Long.MAX_VALUE);
      arrowRecordBatch =
          ArrowConverter.byteArrayToArrowBatch(schema, bufferAllocator);
      vectorSchemaRoot = VectorSchemaRoot
          .create(ArrowUtils.toArrowSchema(carbonSchema, TimeZone.getDefault().getID()),
              bufferAllocator);
      vectorLoader = new VectorLoader(vectorSchemaRoot);
      vectorLoader.load(arrowRecordBatch);
      assertEquals(vectorSchemaRoot.getSchema().getFields().size(), 13);
      arrowRecordBatch.close();
      vectorSchemaRoot.close();
      bufferAllocator.close();
    } catch (Throwable e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    } finally {
      try {
        FileUtils.deleteDirectory(new File(path));
      } catch (IOException e) {
        e.printStackTrace();
        Assert.fail(e.getMessage());
      }
    }
  }
}
