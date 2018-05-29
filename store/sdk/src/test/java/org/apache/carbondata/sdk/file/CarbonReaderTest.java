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

import java.io.*;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.avro.generic.GenericData;
import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.CharEncoding;
import org.junit.*;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

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

    CarbonReader reader = CarbonReader.builder(path, "_temp").isTransactionalTable(true)
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
        .isTransactionalTable(true)
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
  public void testReadColumnTwice() throws IOException, InterruptedException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(new Schema(fields), path, true);

    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .projection(new String[]{"name", "name", "age", "name"})
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
      Assert.assertEquals(name[i], row[1]);
      Assert.assertEquals(age[i], row[2]);
      Assert.assertEquals(name[i], row[3]);
      i++;
    }
    Assert.assertEquals(i, 100);

    reader.close();

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
        .isTransactionalTable(true)
        .build();
    // Reader 2
    CarbonReader reader2 = CarbonReader
        .builder(path, "_temp")
        .projection(new String[]{"name", "age"})
        .isTransactionalTable(true)
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

    CarbonReader reader = CarbonReader.builder(path, "_temp").isTransactionalTable(true)
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
    Schema schema = CarbonSchemaReader.readSchemaInDataFile(dataFiles[0].getAbsolutePath());
    Assert.assertTrue(schema.getFields().length == 2);
    Assert.assertEquals("name", (schema.getFields())[0].getFieldName());
    Assert.assertEquals("age", (schema.getFields())[1].getFieldName());
    Assert.assertEquals(DataTypes.STRING, (schema.getFields())[0].getDataType());
    Assert.assertEquals(DataTypes.INT, (schema.getFields())[1].getDataType());

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

    Schema schema = CarbonSchemaReader.readSchemaInSchemaFile(dataFiles[0].getAbsolutePath());

    // sort the schema
    Arrays.sort(schema.getFields(), new Comparator<Field>() {
      @Override
      public int compare(Field o1, Field o2) {
        return Integer.compare(o1.getSchemaOrdinal(), o2.getSchemaOrdinal());
      }
    });

    // Transform the schema
    String[] strings = new String[schema.getFields().length];
    for (int i = 0; i < schema.getFields().length; i++) {
      strings[i] = (schema.getFields())[i].getFieldName();
    }

    Assert.assertEquals(2, schema.getFields().length);

    Assert.assertEquals("name", (schema.getFields())[0].getFieldName());
    Assert.assertEquals("age", (schema.getFields())[1].getFieldName());
    Assert.assertEquals(DataTypes.STRING, (schema.getFields())[0].getDataType());
    Assert.assertEquals(DataTypes.INT, (schema.getFields())[1].getDataType());

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

    CarbonReader reader = CarbonReader.builder(path, "_temp").isTransactionalTable(true)
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
        .isTransactionalTable(true)
        .projection(new String[]{
            "stringField"
            , "shortField"
            , "intField"
            , "longField"
            , "doubleField"
            , "boolField"
            , "dateField"
            , "timeField"
            , "decimalField"})
        .build();

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

  @Test
  public void testReadSchemaFileAndSort() throws IOException, InterruptedException {
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
    fields[1] = new Field("shortField", DataTypes.SHORT);
    fields[2] = new Field("intField", DataTypes.INT);
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

    File[] dataFiles = new File(path + "/Metadata").listFiles(new FilenameFilter() {
      @Override public boolean accept(File dir, String name) {
        return name.endsWith("schema");
      }
    });
    Schema schema = CarbonSchemaReader.readSchemaInSchemaFile(dataFiles[0].getAbsolutePath());

    // sort the schema
    Arrays.sort(schema.getFields(), new Comparator<Field>() {
      @Override
      public int compare(Field o1, Field o2) {
        return Integer.compare(o1.getSchemaOrdinal(), o2.getSchemaOrdinal());
      }
    });

    // Transform the schema
    String[] strings = new String[schema.getFields().length];
    for (int i = 0; i < schema.getFields().length; i++) {
      strings[i] = (schema.getFields())[i].getFieldName();
    }

    File segmentFolder = new File(CarbonTablePath.getSegmentPath(path, "null"));
    Assert.assertTrue(segmentFolder.exists());

    Assert.assertNotNull(dataFiles);
    Assert.assertTrue(dataFiles.length > 0);

    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .projection(strings)
        .build();

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

  @Test
  public void testReadSchemaInDataFileAndSort() throws IOException, InterruptedException {
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
    fields[1] = new Field("shortField", DataTypes.SHORT);
    fields[2] = new Field("intField", DataTypes.INT);
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

    File[] dataFiles2 = new File(path + "/Fact/Part0/Segment_null/").listFiles(new FilenameFilter() {
      @Override public boolean accept(File dir, String name) {
        return name.endsWith("carbondata");
      }
    });

    Schema schema = CarbonSchemaReader.readSchemaInDataFile(dataFiles2[0].getAbsolutePath());

    // sort the schema
    Arrays.sort(schema.getFields(), new Comparator<Field>() {
      @Override
      public int compare(Field o1, Field o2) {
        return Integer.compare(o1.getSchemaOrdinal(), o2.getSchemaOrdinal());
      }
    });

    // Transform the schema
    String[] strings = new String[schema.getFields().length];
    for (int i = 0; i < schema.getFields().length; i++) {
      strings[i] = (schema.getFields())[i].getFieldName();
    }

    File segmentFolder = new File(CarbonTablePath.getSegmentPath(path, "null"));
    Assert.assertTrue(segmentFolder.exists());

    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .projection(strings)
        .build();

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

  @Test
  public void testReadUserSchema() throws IOException, InterruptedException {
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
    fields[1] = new Field("shortField", DataTypes.SHORT);
    fields[2] = new Field("intField", DataTypes.INT);
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

    File[] dataFiles2 = new File(path + "/Fact/Part0/Segment_null/").listFiles(new FilenameFilter() {
      @Override public boolean accept(File dir, String name) {
        return name.endsWith("carbonindex");
      }
    });

    Schema schema = CarbonSchemaReader.readSchemaInIndexFile(dataFiles2[0].getAbsolutePath()).asOriginOrder();

    // Transform the schema
    String[] strings = new String[schema.getFields().length];
    for (int i = 0; i < schema.getFields().length; i++) {
      strings[i] = (schema.getFields())[i].getFieldName();
    }

    File segmentFolder = new File(CarbonTablePath.getSegmentPath(path, "null"));
    Assert.assertTrue(segmentFolder.exists());

    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .projection(strings)
        .build();

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

  @Test
  public void testReadFilesWithProjectAllColumns() throws IOException, InterruptedException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(new Schema(fields), path, true);

    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .isTransactionalTable(true)
        .projectAllColumns()
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

  @Test
  public void testReadFilesWithDefaultProjection() throws IOException, InterruptedException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(new Schema(fields), path, true);

    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .isTransactionalTable(true)
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
      Assert.assertEquals(name[i], row[0]);
      Assert.assertEquals(age[i], row[1]);
      i++;
    }
    Assert.assertEquals(i, 100);
  }

  @Test
  public void testReadFilesWithNullProjection() throws IOException, InterruptedException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(new Schema(fields), path, true);

    try {
      CarbonReader reader = CarbonReader
          .builder(path, "_temp")
          .projection(new String[]{})
          .isTransactionalTable(true)
          .build();
      assert (false);
    } catch (RuntimeException e) {
      assert (e.getMessage().equalsIgnoreCase("Projection can't be empty"));
    }
  }

  private void WriteAvroComplexData(String mySchema, String json, String[] sortColumns, String path)
      throws IOException, InvalidLoadOptionException {

    // conversion to GenericData.Record
    org.apache.avro.Schema nn = new org.apache.avro.Schema.Parser().parse(mySchema);
    JsonAvroConverter converter = new JsonAvroConverter();
    GenericData.Record record = converter.convertToGenericDataRecord(
        json.getBytes(CharEncoding.UTF_8), nn);

    try {
      CarbonWriter writer = CarbonWriter.builder()
          .outputPath(path)
          .isTransactionalTable(true)
          .buildWriterForAvroInput(nn);

      for (int i = 0; i < 100; i++) {
        writer.write(record);
      }
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  // TODO: support get schema of complex data type
  @Ignore
  public void testReadUserSchemaOfComplex() throws IOException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    String mySchema =
        "{" +
            "  \"name\": \"address\", " +
            "   \"type\": \"record\", " +
            "    \"fields\": [  " +
            "  { \"name\": \"name\", \"type\": \"string\"}, " +
            "  { \"name\": \"age\", \"type\": \"int\"}, " +
            "  { " +
            "    \"name\": \"address\", " +
            "      \"type\": { " +
            "    \"type\" : \"record\", " +
            "        \"name\" : \"my_address\", " +
            "        \"fields\" : [ " +
            "    {\"name\": \"street\", \"type\": \"string\"}, " +
            "    {\"name\": \"city\", \"type\": \"string\"} " +
            "  ]} " +
            "  }, " +
            "  {\"name\" :\"doorNum\", " +
            "   \"type\" : { " +
            "   \"type\" :\"array\", " +
            "   \"items\":{ " +
            "   \"name\" :\"EachdoorNums\", " +
            "   \"type\" : \"int\", " +
            "   \"default\":-1} " +
            "              } " +
            "  }] " +
            "}";

    String json = "{\"name\":\"bob\", \"age\":10, \"address\" : {\"street\":\"abc\", \"city\":\"bang\"}, "
        + "   \"doorNum\" : [1,2,3,4]}";

    try {
      WriteAvroComplexData(mySchema, json, null, path);
    } catch (InvalidLoadOptionException e) {
      e.printStackTrace();
    }

    File segmentFolder = new File(CarbonTablePath.getSegmentPath(path, "null"));
    Assert.assertTrue(segmentFolder.exists());

    File[] dataFiles = segmentFolder.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT);
      }
    });
    Assert.assertNotNull(dataFiles);
    Assert.assertEquals(1, dataFiles.length);


    File[] dataFiles2 = new File(path + "/Fact/Part0/Segment_null/").listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith("carbonindex");
      }
    });

    Schema schema = CarbonSchemaReader.readSchemaInIndexFile(dataFiles2[0].getAbsolutePath()).asOriginOrder();

    for (int i = 0; i < schema.getFields().length; i++) {
      System.out.println((schema.getFields())[i].getFieldName() + "\t" + schema.getFields()[i].getSchemaOrdinal());
    }
    FileUtils.deleteDirectory(new File(path));
  }

}
