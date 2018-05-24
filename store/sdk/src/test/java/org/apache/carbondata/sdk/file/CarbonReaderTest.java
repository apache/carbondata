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
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CarbonReaderTest extends TestCase {

  @Before
  public void cleanFile() {
    assert (TestUtil.cleanMdtFile());
  }

  @After
  public void verifyDMFile() {
    assert (!TestUtil.verifyMdtFile());
  }

  @Test
  public void testWriteAndReadFiles() throws IOException, InterruptedException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(new Schema(fields), path, true);

    CarbonReader reader = CarbonReader.builder(path, "_temp")
        .projection(new String[]{"name", "age"}).build();

    // expected output after sorting
    String[] name = new String[100];
    int[] age = new int[100];
    for (int i = 0; i < 100; i++) {
      name[i] = "robot" + (i / 10);
      age[i] = (i % 10) * 10 + i / 10;
    }

    int i = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      // Default sort column is applied for dimensions. So, need  to validate accordingly
      Assert.assertEquals(name[i], row[0]);
      Assert.assertEquals(age[i], row[1]);
      i++;
    }
    Assert.assertEquals(i, 100);

    reader.close();

    // Read again
    CarbonReader reader2 = CarbonReader
        .builder(path, "_temp")
        .projection(new String[]{"name", "age"})
        .build();

    i = 0;
    while (reader2.hasNext()) {
      Object[] row = (Object[]) reader2.readNextRow();
      // Default sort column is applied for dimensions. So, need  to validate accordingly
      Assert.assertEquals(name[i], row[0]);
      Assert.assertEquals(age[i], row[1]);
      i++;
    }
    Assert.assertEquals(i, 100);
    reader2.close();

    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testReadFilesParallel() throws IOException, InterruptedException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(new Schema(fields), path, true);

    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .projection(new String[]{"name", "age"})
        .build();
    // Reader 2
    CarbonReader reader2 = CarbonReader
        .builder(path, "_temp")
        .projection(new String[]{"name", "age"})
        .build();

    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      Object[] row2 = (Object[]) reader2.readNextRow();
      // parallel compare
      Assert.assertEquals(row[0], row2[0]);
      Assert.assertEquals(row[1], row2[1]);
    }

    reader.close();
    reader2.close();

    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testReadAfterClose() throws IOException, InterruptedException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(new Schema(fields), path, true);

    CarbonReader reader = CarbonReader.builder(path, "_temp")
        .projection(new String[]{"name", "age"}).build();

    reader.close();
    String msg = "CarbonReader not initialise, please create it first.";
    try {
      reader.hasNext();
      assert (false);
    } catch (RuntimeException e) {
      assert (e.getMessage().equals(msg));
    }

    try {
      reader.readNextRow();
      assert (false);
    } catch (RuntimeException e) {
      assert (e.getMessage().equals(msg));
    }

    try {
      reader.close();
      assert (false);
    } catch (RuntimeException e) {
      assert (e.getMessage().equals(msg));
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


  @Test
  public void testWriteAndReadFilesNonTransactional() throws IOException, InterruptedException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    // Write to a Non Transactional Table
    TestUtil.writeFilesAndVerify(new Schema(fields), path, true, false);

    CarbonReader reader = CarbonReader.builder(path, "_temp")
        .projection(new String[]{"name", "age"})
        .isTransactionalTable(false)
        .build();

    // expected output after sorting
    String[] name = new String[100];
    int[] age = new int[100];
    for (int i = 0; i < 100; i++) {
      name[i] = "robot" + (i / 10);
      age[i] = (i % 10) * 10 + i / 10;
    }

    int i = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      // Default sort column is applied for dimensions. So, need  to validate accordingly
      Assert.assertEquals(name[i], row[0]);
      Assert.assertEquals(age[i], row[1]);
      i++;
    }
    Assert.assertEquals(i, 100);
    reader.close();
    FileUtils.deleteDirectory(new File(path));
  }

  CarbonProperties carbonProperties;

  @Override
  public void setUp() {
    carbonProperties = CarbonProperties.getInstance();
  }

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonReaderTest.class.getName());

  @Test
  public void testTimeStampAndBadRecord() throws IOException, InterruptedException {
    String timestampFormat = carbonProperties.getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
    String badRecordAction = carbonProperties.getProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION,
        CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION_DEFAULT);
    String badRecordLoc = carbonProperties.getProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC,
        CarbonCommonConstants.CARBON_BADRECORDS_LOC_DEFAULT_VAL);
    String rootPath = new File(this.getClass().getResource("/").getPath()
        + "../../").getCanonicalPath();
    String storeLocation = rootPath + "/target/";
    carbonProperties
        .addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC, storeLocation)
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd hh:mm:ss")
        .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, "REDIRECT");
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[9];
    fields[0] = new Field("stringField", DataTypes.STRING);
    fields[1] = new Field("intField", DataTypes.INT);
    fields[2] = new Field("shortField", DataTypes.SHORT);
    fields[3] = new Field("longField", DataTypes.LONG);
    fields[4] = new Field("doubleField", DataTypes.DOUBLE);
    fields[5] = new Field("boolField", DataTypes.BOOLEAN);
    fields[6] = new Field("dateField", DataTypes.DATE);
    fields[7] = new Field("timeField", DataTypes.TIMESTAMP);
    fields[8] = new Field("decimalField", DataTypes.createDecimalType(8, 2));

    try {
      CarbonWriterBuilder builder = CarbonWriter.builder()
          .isTransactionalTable(true)
          .persistSchemaFile(true)
          .outputPath(path);

      CarbonWriter writer = builder.buildWriterForCSVInput(new Schema(fields));

      for (int i = 0; i < 100; i++) {
        String[] row = new String[]{
            "robot" + (i % 10),
            String.valueOf(i),
            String.valueOf(i),
            String.valueOf(Long.MAX_VALUE - i),
            String.valueOf((double) i / 2),
            String.valueOf(true),
            "2018-05-12",
            "2018-05-12",
            "12.345"
        };
        writer.write(row);
        String[] row2 = new String[]{
            "robot" + (i % 10),
            String.valueOf(i),
            String.valueOf(i),
            String.valueOf(Long.MAX_VALUE - i),
            String.valueOf((double) i / 2),
            String.valueOf(true),
            "2019-03-02",
            "2019-02-12 03:03:34",
            "12.345"
        };
        writer.write(row2);
      }
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
    LOGGER.audit("Bad record location:" + storeLocation);
    File segmentFolder = new File(CarbonTablePath.getSegmentPath(path, "null"));
    Assert.assertTrue(segmentFolder.exists());

    File[] dataFiles = segmentFolder.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT);
      }
    });
    Assert.assertNotNull(dataFiles);
    Assert.assertTrue(dataFiles.length > 0);

    CarbonReader reader = CarbonReader.builder(path, "_temp")
        .projection(new String[]{
            "stringField"
            , "shortField"
            , "intField"
            , "longField"
            , "doubleField"
            , "boolField"
            , "dateField"
            , "timeField"
            , "decimalField"}).build();

    int i = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      int id = (int) row[2];
      Assert.assertEquals("robot" + (id % 10), row[0]);
      Assert.assertEquals(Short.parseShort(String.valueOf(id)), row[1]);
      Assert.assertEquals(Long.MAX_VALUE - id, row[3]);
      Assert.assertEquals((double) id / 2, row[4]);
      Assert.assertEquals(true, (boolean) row[5]);
      long day = 24L * 3600 * 1000;
      Assert.assertEquals("2019-03-02", new Date((day * ((int) row[6]))).toString());
      Assert.assertEquals("2019-02-12 03:03:34.0", new Timestamp((long) row[7] / 1000).toString());
      i++;
    }
    Assert.assertEquals(i, 100);

    reader.close();
    FileUtils.deleteDirectory(new File(path));
    carbonProperties.addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        timestampFormat);
    carbonProperties.addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION,
        badRecordAction);
    carbonProperties.addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC,
        badRecordLoc);
  }

}
