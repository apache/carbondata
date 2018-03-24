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
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;

import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

public class CarbonReaderTest {

  @Test
  public void testWriteAndReadFiles() throws IOException, InterruptedException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(new Schema(fields), path, true);

    CarbonReader reader = CarbonReader.builder(path)
        .projection(new String[]{"name", "age"}).build();

    int i = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[])reader.readNextRow();
      Assert.assertEquals("robot" + (i % 10), row[0]);
      Assert.assertEquals(i, row[1]);
      i++;
    }

    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testReadSchemaFromDataFile() throws IOException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(new Schema(fields), path, true);

    File[] dataFiles = new File(path + "/Fact/Part0/Segment_null/").listFiles(new FilenameFilter() {
      @Override public boolean accept(File dir, String name) {
        return name.endsWith("carbondata");
      }
    });
    Assert.assertTrue(dataFiles != null);
    Assert.assertTrue(dataFiles.length > 0);
    List<ColumnSchema> columns = CarbonReader.readSchemaInDataFile(dataFiles[0].getAbsolutePath());
    Assert.assertTrue(columns.size() == 2);
    Assert.assertEquals("name", columns.get(0).getColumnName());
    Assert.assertEquals("age", columns.get(1).getColumnName());
    Assert.assertEquals(DataTypes.STRING, columns.get(0).getDataType());
    Assert.assertEquals(DataTypes.INT, columns.get(1).getDataType());

    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testReadSchemaFromSchemaFile() throws IOException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(new Schema(fields), path, true);

    File[] dataFiles = new File(path + "/Metadata").listFiles(new FilenameFilter() {
      @Override public boolean accept(File dir, String name) {
        return name.endsWith("schema");
      }
    });
    Assert.assertTrue(dataFiles != null);
    Assert.assertTrue(dataFiles.length > 0);
    TableInfo tableInfo = CarbonReader.readSchemaFile(dataFiles[0].getAbsolutePath());
    Assert.assertEquals(2, tableInfo.getFactTable().getListOfColumns().size());

    List<ColumnSchema> columns = tableInfo.getFactTable().getListOfColumns();
    Assert.assertEquals(2, columns.size());
    Assert.assertEquals("name", columns.get(0).getColumnName());
    Assert.assertEquals("age", columns.get(1).getColumnName());
    Assert.assertEquals(DataTypes.STRING, columns.get(0).getDataType());
    Assert.assertEquals(DataTypes.INT, columns.get(1).getDataType());

    FileUtils.deleteDirectory(new File(path));
  }
}
