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
import java.io.FileFilter;
import java.io.IOException;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.avro.generic.GenericData;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.CharEncoding;
import org.junit.Assert;
import org.junit.Test;

import tech.allegro.schema.json2avro.converter.JsonAvroConverter;
import org.apache.avro.Schema;

public class AvroCarbonWriterTest {
  private String path = "./AvroCarbonWriterSuiteWriteFiles";

  @Test
  public void testWriteBasic() throws IOException {
    FileUtils.deleteDirectory(new File(path));

    // Avro schema
    String avroSchema =
        "{" +
            "   \"type\" : \"record\"," +
            "   \"name\" : \"Acme\"," +
            "   \"fields\" : ["
            + "{ \"name\" : \"name\", \"type\" : \"string\" },"
            + "{ \"name\" : \"age\", \"type\" : \"int\" }]" +
        "}";

    String json = "{\"name\":\"bob\", \"age\":10}";

    // conversion to GenericData.Record
    JsonAvroConverter converter = new JsonAvroConverter();
    GenericData.Record record = converter.convertToGenericDataRecord(
        json.getBytes(CharEncoding.UTF_8), new Schema.Parser().parse(avroSchema));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.STRING);

    try {
      CarbonWriter writer = CarbonWriter.builder()
          .withSchema(new org.apache.carbondata.sdk.file.Schema(fields))
          .outputPath(path)
          .buildWriterForAvroInput();

      for (int i = 0; i < 100; i++) {
        writer.write(record);
      }
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    File segmentFolder = new File(CarbonTablePath.getSegmentPath(path, "null"));
    Assert.assertTrue(segmentFolder.exists());

    File[] dataFiles = segmentFolder.listFiles(new FileFilter() {
      @Override public boolean accept(File pathname) {
        return pathname.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT);
      }
    });
    Assert.assertNotNull(dataFiles);
    Assert.assertEquals(1, dataFiles.length);

    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testWriteAllPrimitive() throws IOException {
    // TODO
  }

  @Test
  public void testWriteNestedRecord() throws IOException {
    // TODO
  }

}
