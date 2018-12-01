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
import java.util.*;

import org.apache.avro.generic.GenericData;
import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.log4j.Logger;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.*;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.expression.logical.OrExpression;
import org.apache.carbondata.core.util.CarbonProperties;

import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.junit.*;

public class CarbonReaderTest extends TestCase {

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
  public void testWriteAndReadFiles() throws IOException, InterruptedException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));
    DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(path));
    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(200, new Schema(fields), path);

    CarbonReader reader = CarbonReader.builder(path, "_temp")
        .projection(new String[]{"name", "age"}).build();

    // expected output after sorting
    String[] name = new String[200];
    Integer[] age = new Integer[200];
    for (int i = 0; i < 200; i++) {
      name[i] = "robot" + (i / 10);
      age[i] = i;
    }

    int i = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      // Default sort column is applied for dimensions. So, need  to validate accordingly
      assert(Arrays.asList(name).contains(row[0]));
      assert(Arrays.asList(age).contains(row[1]));
      i++;
    }
    Assert.assertEquals(i, 200);

    // Read again
    CarbonReader reader2 = CarbonReader
        .builder(path, "_temp")
        .projection(new String[]{"name", "age"})
        .build();

    i = 0;
    while (reader2.hasNext()) {
      Object[] row = (Object[]) reader2.readNextRow();
      // Default sort column is applied for dimensions. So, need  to validate accordingly
      assert(Arrays.asList(name).contains(row[0]));
      assert(Arrays.asList(age).contains(row[1]));
      i++;
    }
    Assert.assertEquals(i, 200);
    reader2.close();
    reader.close();

    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testReadWithFilterOfNonTransactionalSimple() throws IOException, InterruptedException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));
    DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(path));
    String path1 = path + "/0testdir";
    String path2 = path + "/testdir";

    FileUtils.deleteDirectory(new File(path));

    FileFactory.getCarbonFile(path1, FileFactory.getFileType(path1));
    FileFactory.mkdirs(path1, FileFactory.getFileType(path1));

    FileFactory.getCarbonFile(path2, FileFactory.getFileType(path2));
    FileFactory.mkdirs(path2, FileFactory.getFileType(path2));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(200, new Schema(fields), path);

    ColumnExpression columnExpression = new ColumnExpression("name", DataTypes.STRING);
    EqualToExpression equalToExpression = new EqualToExpression(columnExpression,
        new LiteralExpression("robot1", DataTypes.STRING));

    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .projection(new String[]{"name", "age"})
        .filter(equalToExpression)
        .build();

    int i = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      // Default sort column is applied for dimensions. So, need  to validate accordingly
      assert ("robot1".equals(row[0]));
      i++;
    }
    Assert.assertEquals(i, 20);

    reader.close();

    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testReadWithFilterOfNonTransactional2() throws IOException, InterruptedException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));
    DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(path));
    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(200, new Schema(fields), path);

    ColumnExpression columnExpression = new ColumnExpression("age", DataTypes.INT);

    EqualToExpression equalToExpression = new EqualToExpression(columnExpression,
        new LiteralExpression("1", DataTypes.INT));
    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .projection(new String[]{"name", "age"})
        .filter(equalToExpression)
        .build();

    int i = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      // Default sort column is applied for dimensions. So, need  to validate accordingly
      assert (((String) row[0]).contains("robot"));
      assert (1 == (int) (row[1]));
      i++;
    }
    Assert.assertEquals(i, 1);

    reader.close();

    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testReadWithFilterOfNonTransactionalAnd() throws IOException, InterruptedException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));
    DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(path));
    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("doubleField", DataTypes.DOUBLE);

    TestUtil.writeFilesAndVerify(200, new Schema(fields), path);

    ColumnExpression columnExpression = new ColumnExpression("doubleField", DataTypes.DOUBLE);
    EqualToExpression equalToExpression = new EqualToExpression(columnExpression,
        new LiteralExpression("3.5", DataTypes.DOUBLE));

    ColumnExpression columnExpression2 = new ColumnExpression("name", DataTypes.STRING);
    EqualToExpression equalToExpression2 = new EqualToExpression(columnExpression2,
        new LiteralExpression("robot7", DataTypes.STRING));

    AndExpression andExpression = new AndExpression(equalToExpression, equalToExpression2);
    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .projection(new String[]{"name", "age", "doubleField"})
        .filter(andExpression)
        .build();

    int i = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      assert (((String) row[0]).contains("robot7"));
      assert (7 == (int) (row[1]));
      assert (3.5 == (double) (row[2]));
      i++;
    }
    Assert.assertEquals(i, 1);

    reader.close();

    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testReadWithFilterOfNonTransactionalOr() throws IOException, InterruptedException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));
    DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(path));
    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("doubleField", DataTypes.DOUBLE);

    TestUtil.writeFilesAndVerify(200, new Schema(fields), path);

    ColumnExpression columnExpression = new ColumnExpression("doubleField", DataTypes.DOUBLE);
    EqualToExpression equalToExpression = new EqualToExpression(columnExpression,
        new LiteralExpression("3.5", DataTypes.DOUBLE));

    ColumnExpression columnExpression2 = new ColumnExpression("name", DataTypes.STRING);
    EqualToExpression equalToExpression2 = new EqualToExpression(columnExpression2,
        new LiteralExpression("robot7", DataTypes.STRING));

    OrExpression orExpression = new OrExpression(equalToExpression, equalToExpression2);
    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .projection(new String[]{"name", "age", "doubleField"})
        .filter(orExpression)
        .build();

    int i = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      assert (((String) row[0]).contains("robot7"));
      assert (7 == ((int) (row[1]) % 10));
      assert (0.5 == ((double) (row[2]) % 1));
      i++;
    }
    Assert.assertEquals(i, 20);

    reader.close();

    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testReadWithFilterOfNonTransactionalGreaterThan() throws IOException, InterruptedException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));
    DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(path));
    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("doubleField", DataTypes.DOUBLE);

    TestUtil.writeFilesAndVerify(200, new Schema(fields), path);

    ColumnExpression columnExpression = new ColumnExpression("doubleField", DataTypes.DOUBLE);
    GreaterThanExpression greaterThanExpression = new GreaterThanExpression(columnExpression,
        new LiteralExpression("13.5", DataTypes.DOUBLE));

    ColumnExpression columnExpression2 = new ColumnExpression("name", DataTypes.STRING);
    EqualToExpression equalToExpression2 = new EqualToExpression(columnExpression2,
        new LiteralExpression("robot7", DataTypes.STRING));

    AndExpression andExpression = new AndExpression(greaterThanExpression, equalToExpression2);
    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .projection(new String[]{"name", "age", "doubleField"})
        .filter(andExpression)
        .build();

    int i = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      assert (((String) row[0]).contains("robot7"));
      assert (7 == ((int) (row[1]) % 10));
      assert ((double) row[2] > 13.5);
      i++;
    }
    Assert.assertEquals(i, 17);

    reader.close();

    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testReadWithFilterOfNonTransactionalLessThan() throws IOException, InterruptedException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));
    DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(path));
    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("doubleField", DataTypes.DOUBLE);

    TestUtil.writeFilesAndVerify(200, new Schema(fields), path);

    ColumnExpression columnExpression = new ColumnExpression("doubleField", DataTypes.DOUBLE);
    LessThanExpression lessThanExpression = new LessThanExpression(columnExpression,
        new LiteralExpression("13.5", DataTypes.DOUBLE));

    ColumnExpression columnExpression2 = new ColumnExpression("name", DataTypes.STRING);
    EqualToExpression equalToExpression2 = new EqualToExpression(columnExpression2,
        new LiteralExpression("robot7", DataTypes.STRING));

    AndExpression andExpression = new AndExpression(lessThanExpression, equalToExpression2);
    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .projection(new String[]{"name", "age", "doubleField"})
        .filter(andExpression)
        .build();

    int i = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      assert (((String) row[0]).contains("robot7"));
      assert (7 == ((int) (row[1]) % 10));
      assert ((double) row[2] < 13.5);
      i++;
    }
    Assert.assertEquals(i, 2);

    reader.close();

    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testReadWithFilterOfNonTransactionalNotEqual() throws IOException, InterruptedException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));
    DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(path));
    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("doubleField", DataTypes.DOUBLE);

    TestUtil.writeFilesAndVerify(200, new Schema(fields), path);

    ColumnExpression columnExpression = new ColumnExpression("doubleField", DataTypes.DOUBLE);
    LessThanExpression lessThanExpression = new LessThanExpression(columnExpression,
        new LiteralExpression("13.5", DataTypes.DOUBLE));

    ColumnExpression columnExpression2 = new ColumnExpression("name", DataTypes.STRING);
    NotEqualsExpression notEqualsExpression = new NotEqualsExpression(columnExpression2,
        new LiteralExpression("robot7", DataTypes.STRING));

    AndExpression andExpression = new AndExpression(lessThanExpression, notEqualsExpression);
    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .projection(new String[]{"name", "age", "doubleField"})
        .filter(andExpression)
        .build();

    int i = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      assert (!((String) row[0]).contains("robot7"));
      assert (7 != ((int) (row[1]) % 10));
      assert ((double) row[2] < 13.5);
      i++;
    }
    Assert.assertEquals(i, 25);

    reader.close();

    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testReadWithFilterOfNonTransactionalIn() throws IOException, InterruptedException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("doubleField", DataTypes.DOUBLE);

    TestUtil.writeFilesAndVerify(200, new Schema(fields), path);
    DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(path));
    ColumnExpression columnExpression = new ColumnExpression("doubleField", DataTypes.DOUBLE);
    LessThanExpression lessThanExpression = new LessThanExpression(columnExpression,
        new LiteralExpression("13.5", DataTypes.DOUBLE));

    ColumnExpression columnExpression2 = new ColumnExpression("name", DataTypes.STRING);
    InExpression inExpression = new InExpression(columnExpression2,
        new LiteralExpression("robot7", DataTypes.STRING));

    AndExpression andExpression = new AndExpression(lessThanExpression, inExpression);
    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .projection(new String[]{"name", "age", "doubleField"})
        .filter(andExpression)
        .build();

    int i = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      assert (((String) row[0]).contains("robot7"));
      assert (7 == ((int) (row[1]) % 10));
      assert ((double) row[2] < 13.5);
      i++;
    }
    Assert.assertEquals(i, 2);

    reader.close();

    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testReadWithFilterOfNonTransactionalNotIn() throws IOException, InterruptedException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));
    DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(path));
    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("doubleField", DataTypes.DOUBLE);

    TestUtil.writeFilesAndVerify(200, new Schema(fields), path);

    ColumnExpression columnExpression = new ColumnExpression("doubleField", DataTypes.DOUBLE);
    LessThanExpression lessThanExpression = new LessThanExpression(columnExpression,
        new LiteralExpression("13.5", DataTypes.DOUBLE));

    ColumnExpression columnExpression2 = new ColumnExpression("name", DataTypes.STRING);
    NotInExpression notInExpression = new NotInExpression(columnExpression2,
        new LiteralExpression("robot7", DataTypes.STRING));

    AndExpression andExpression = new AndExpression(lessThanExpression, notInExpression);
    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .projection(new String[]{"name", "age", "doubleField"})
        .filter(andExpression)
        .build();

    int i = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      assert (!((String) row[0]).contains("robot7"));
      assert (7 != ((int) (row[1]) % 10));
      assert ((double) row[2] < 13.5);
      i++;
    }
    Assert.assertEquals(i, 25);

    reader.close();

    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testWriteAndReadFilesWithReaderBuildFail() throws IOException, InterruptedException {
    String path1 = "./testWriteFiles";
    String path2 = "./testWriteFiles2";
    FileUtils.deleteDirectory(new File(path1));
    FileUtils.deleteDirectory(new File(path2));
    DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(path1));
    DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(path2));
    Field[] fields = new Field[] { new Field("c1", "string"),
         new Field("c2", "int") };
    Schema schema = new Schema(fields);
    CarbonWriterBuilder builder = CarbonWriter.builder();
    CarbonWriter carbonWriter = null;
    try {
      carbonWriter = builder.outputPath(path1).uniqueIdentifier(12345)
  .withCsvInput(schema).writtenBy("CarbonReaderTest").build();
    } catch (InvalidLoadOptionException e) {
      e.printStackTrace();
    }
    carbonWriter.write(new String[] { "MNO", "100" });
    carbonWriter.close();

    Field[] fields1 = new Field[] { new Field("p1", "string"),
         new Field("p2", "int") };
    Schema schema1 = new Schema(fields1);
    CarbonWriterBuilder builder1 = CarbonWriter.builder();
    CarbonWriter carbonWriter1 = null;
    try {
      carbonWriter1 = builder1.outputPath(path2).uniqueIdentifier(12345)
   .withCsvInput(schema1).writtenBy("CarbonReaderTest").build();
    } catch (InvalidLoadOptionException e) {
      e.printStackTrace();
    }
    carbonWriter1.write(new String[] { "PQR", "200" });
    carbonWriter1.close();

    try {
       CarbonReader reader =
       CarbonReader.builder(path1, "_temp").
       projection(new String[] { "c1", "c3" })
       .build();
    } catch (Exception e){
       System.out.println("Success");
    }
    CarbonReader reader1 =
         CarbonReader.builder(path2, "_temp1")
     .projection(new String[] { "p1", "p2" })
     .build();

    while (reader1.hasNext()) {
       Object[] row1 = (Object[]) reader1.readNextRow();
       System.out.println(row1[0]);
       System.out.println(row1[1]);
    }
    reader1.close();

    FileUtils.deleteDirectory(new File(path1));
    FileUtils.deleteDirectory(new File(path2));
  }

  @Test
  public void testReadColumnTwice() throws IOException, InterruptedException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));
    DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(path));
    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(100, new Schema(fields), path);

    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .projection(new String[]{"name", "age", "age", "name"})
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
      Assert.assertEquals(age[i], row[2]);
      Assert.assertEquals(name[i], row[3]);
      i++;
    }
    Assert.assertEquals(i, 100);

    reader.close();

    FileUtils.deleteDirectory(new File(path));
  }

  // Below test case was working with transactional table as schema file was present.
  // now we don't support transactional table from SDK. only flat folder is supported.
  // and currently flat folder will never check for schema files.
  @Ignore
  public void readFilesParallel() throws IOException, InterruptedException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));
    DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(path));
    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(100, new Schema(fields), path);

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
    DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(path));
    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(100, new Schema(fields), path);

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
  public void testWriteAndReadFilesWithoutTableName() throws IOException, InterruptedException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));
    DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(path));
    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(100, new Schema(fields), path);

    CarbonReader reader = CarbonReader
        .builder(path)
        .projection(new String[]{"name", "age"})
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
  public void testWriteAndReadFilesWithoutTableName2() throws IOException, InterruptedException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));
    DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(path));
    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(new Schema(fields), path);

    CarbonReader reader = CarbonReader.builder(path).build();

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
  public void testReadSchemaFromDataFile() throws IOException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(100, new Schema(fields), path);

    File[] dataFiles = new File(path).listFiles(new FilenameFilter() {
      @Override public boolean accept(File dir, String name) {
        return name.endsWith("carbondata");
      }
    });
    Assert.assertTrue(dataFiles != null);
    Assert.assertTrue(dataFiles.length > 0);
    Schema schema = CarbonSchemaReader.readSchema(dataFiles[0].getAbsolutePath());
    Assert.assertTrue(schema.getFields().length == 2);
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
    TestUtil.writeFilesAndVerify(new Schema(fields), path);

    CarbonReader reader = CarbonReader.builder(path, "_temp")
        .projection(new String[]{"name", "age"})
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
    String path = null;
    try {
      path = new File(CarbonReaderTest.class.getResource("/").getPath() + "../")
          .getCanonicalPath().replaceAll("\\\\", "/");
    } catch (IOException e) {
      assert (false);
    }
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_SYSTEM_FOLDER_LOCATION, path);
  }

  private static final Logger LOGGER =
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
      CarbonWriterBuilder builder = CarbonWriter.builder().outputPath(path);

      CarbonWriter writer = builder.withCsvInput(new Schema(fields)).writtenBy("CarbonReaderTest").build();

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
    File folder = new File(path);
    Assert.assertTrue(folder.exists());

    File[] dataFiles = folder.listFiles(new FileFilter() {
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
      CarbonWriterBuilder builder = CarbonWriter.builder().outputPath(path);

      CarbonWriter writer = builder.withCsvInput(new Schema(fields)).writtenBy("CarbonReaderTest").build();

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

    File[] dataFiles2 = new File(path).listFiles(new FilenameFilter() {
      @Override public boolean accept(File dir, String name) {
        return name.endsWith("carbondata");
      }
    });

    Schema schema = CarbonSchemaReader.readSchema(dataFiles2[0].getAbsolutePath());

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

    File folder = new File(path);
    Assert.assertTrue(folder.exists());

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
      CarbonWriterBuilder builder = CarbonWriter.builder().outputPath(path)
          .writtenBy("SDK_1.0.0");

      CarbonWriter writer = builder.withCsvInput(new Schema(fields)).build();

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

    File[] dataFiles1 = new File(path).listFiles(new FilenameFilter() {
      @Override public boolean accept(File dir, String name) {
        return name.endsWith("carbondata");
      }
    });
    String versionDetails = CarbonSchemaReader.getVersionDetails(dataFiles1[0].getAbsolutePath());
    assertTrue(versionDetails.contains("SDK_1.0.0 in version: "));

    File[] dataFiles2 = new File(path).listFiles(new FilenameFilter() {
      @Override public boolean accept(File dir, String name) {
        return name.endsWith("carbonindex");
      }
    });

    Schema schema = CarbonSchemaReader.readSchema(dataFiles2[0].getAbsolutePath()).asOriginOrder();
    // Transform the schema
    String[] strings = new String[schema.getFields().length];
    for (int i = 0; i < schema.getFields().length; i++) {
      strings[i] = (schema.getFields())[i].getFieldName();
    }

    File folder = new File(path);
    Assert.assertTrue(folder.exists());

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

    TestUtil.writeFilesAndVerify(100, new Schema(fields), path);

    CarbonReader reader = CarbonReader.builder(path, "_temp").build();

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

    TestUtil.writeFilesAndVerify(100, new Schema(fields), path);

    CarbonReader reader = CarbonReader.builder(path, "_temp").build();

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
    reader.close();
    Assert.assertEquals(i, 100);
  }

  @Test
  public void testReadFilesWithNullProjection() throws IOException, InterruptedException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(100, new Schema(fields), path);

    try {
      CarbonReader reader = CarbonReader
          .builder(path, "_temp")
          .projection(new String[]{})
          .build();
      assert (false);
    } catch (RuntimeException e) {
      assert (e.getMessage().equalsIgnoreCase("Projection can't be empty"));
    }
  }

  private void WriteAvroComplexData(String mySchema, String json, String path)
      throws IOException, InvalidLoadOptionException {

    // conversion to GenericData.Record
    org.apache.avro.Schema nn = new org.apache.avro.Schema.Parser().parse(mySchema);
    GenericData.Record record = TestUtil.jsonToAvro(json, mySchema);

    try {
      CarbonWriter writer = CarbonWriter.builder()
          .outputPath(path)
          .withAvroInput(nn).writtenBy("CarbonReaderTest").build();

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
      WriteAvroComplexData(mySchema, json, path);
    } catch (InvalidLoadOptionException e) {
      e.printStackTrace();
    }

    File folder = new File(path);
    Assert.assertTrue(folder.exists());

    File[] dataFiles = folder.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT);
      }
    });
    Assert.assertNotNull(dataFiles);
    Assert.assertEquals(1, dataFiles.length);


    File[] dataFiles2 = new File(path).listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith("carbonindex");
      }
    });

    Schema schema = CarbonSchemaReader.readSchema(dataFiles2[0].getAbsolutePath()).asOriginOrder();

    for (int i = 0; i < schema.getFields().length; i++) {
      System.out.println((schema.getFields())[i].getFieldName() + "\t" + schema.getFields()[i].getSchemaOrdinal());
    }
    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testReadMapType() throws IOException, InterruptedException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    String mySchema =
        "{ "+
            "  \"name\": \"address\", "+
            "  \"type\": \"record\", "+
            "  \"fields\": [ "+
            "    { "+
            "      \"name\": \"name\", "+
            "      \"type\": \"string\" "+
            "    }, "+
            "    { "+
            "      \"name\": \"age\", "+
            "      \"type\": \"int\" "+
            "    }, "+
            "    { "+
            "      \"name\": \"mapRecord\", "+
            "      \"type\": { "+
            "        \"type\": \"map\", "+
            "        \"values\": \"string\" "+
            "      } "+
            "    } "+
            "  ] "+
            "} ";

    String json =
        "{\"name\":\"bob\", \"age\":10, \"mapRecord\": {\"street\": \"k-lane\", \"city\": \"bangalore\"}}";

    try {
      WriteAvroComplexData(mySchema, json, path);
    } catch (InvalidLoadOptionException e) {
      e.printStackTrace();
    }

    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("mapRecord", DataTypes.createMapType(DataTypes.STRING, DataTypes.STRING));

    CarbonReader reader = CarbonReader.builder(path, "_temp").build();

    // expected output
    String name = "bob";
    int age = 10;
    Object[] mapKeValue = new Object[2];
    mapKeValue[0] = new Object[] { "city", "street" };
    mapKeValue[1] = new Object[] { "bangalore", "k-lane" };
    int i = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      Assert.assertEquals(name, row[0]);
      Assert.assertArrayEquals(mapKeValue, (Object[]) row[1]);
      Assert.assertEquals(age, row[2]);
      i++;
    }
    reader.close();
    Assert.assertEquals(i, 100);
  }

  @Test
  public void testReadWithFilterOfnonTransactionalwithsubfolders() throws IOException, InterruptedException {
    String path1 = "./testWriteFiles/1/"+System.nanoTime();
    String path2 = "./testWriteFiles/2/"+System.nanoTime();
    String path3 = "./testWriteFiles/3/"+System.nanoTime();
    FileUtils.deleteDirectory(new File("./testWriteFiles"));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(200, new Schema(fields), path1);
    TestUtil.writeFilesAndVerify(200, new Schema(fields), path2);
    TestUtil.writeFilesAndVerify(200, new Schema(fields), path3);

    EqualToExpression equalToExpression = new EqualToExpression(
        new ColumnExpression("name", DataTypes.STRING),
        new LiteralExpression("robot1", DataTypes.STRING));
    CarbonReader reader = CarbonReader
        .builder("./testWriteFiles", "_temp")
        .projection(new String[]{"name", "age"})
        .filter(equalToExpression)
        .build();

    int i = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      // Default sort column is applied for dimensions. So, need  to validate accordingly
      assert ("robot1".equals(row[0]));
      i++;
    }
    Assert.assertEquals(i, 60);

    reader.close();

    FileUtils.deleteDirectory(new File("./testWriteFiles"));
  }

  @Test
  public void testReadSchemaFromDataFileArrayString() {
    String path = "./testWriteFiles";
    try {
      FileUtils.deleteDirectory(new File(path));

      Field[] fields = new Field[11];
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
      Map<String, String> map = new HashMap<>();
      map.put("complex_delimiter_level_1", "#");
      CarbonWriter writer = CarbonWriter.builder()
          .outputPath(path)
          .withLoadOptions(map)
          .withCsvInput(new Schema(fields))
          .writtenBy("CarbonReaderTest")
          .build();

      for (int i = 0; i < 10; i++) {
        String[] row2 = new String[]{
            "robot" + (i % 10),
            String.valueOf(i % 10000),
            String.valueOf(i),
            String.valueOf(Long.MAX_VALUE - i),
            String.valueOf((double) i / 2),
            String.valueOf(true),
            "2019-03-02",
            "2019-02-12 03:03:34",
            "12.345",
            "varchar",
            "Hello#World#From#Carbon"
        };
        writer.write(row2);
      }
      writer.close();

      File[] dataFiles = new File(path).listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          if (name == null) {
            return false;
          }
          return name.endsWith("carbondata");
        }
      });
      if (dataFiles == null || dataFiles.length < 1) {
        throw new RuntimeException("Carbon data file not exists.");
      }
      Schema schema = CarbonSchemaReader
          .readSchema(dataFiles[0].getAbsolutePath())
          .asOriginOrder();
      // Transform the schema
      String[] strings = new String[schema.getFields().length];
      for (int i = 0; i < schema.getFields().length; i++) {
        strings[i] = (schema.getFields())[i].getFieldName();
      }

      // Read data
      CarbonReader reader = CarbonReader
          .builder(path, "_temp")
          .projection(strings)
          .build();

      int i = 0;
      while (reader.hasNext()) {
        Object[] row = (Object[]) reader.readNextRow();
        assert (row[0].equals("robot" + i));
        assert (row[2].equals(i));
        assert (row[6].equals(17957));
        Object[] arr = (Object[]) row[10];
        assert (arr[0].equals("Hello"));
        assert (arr[3].equals("Carbon"));
        i++;
      }
      reader.close();
      FileUtils.deleteDirectory(new File(path));
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }

   @Test
  public void testReadNextRowWithRowUtil() {
    String path = "./carbondata";
    try {
      FileUtils.deleteDirectory(new File(path));

      Field[] fields = new Field[12];
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
      Map<String, String> map = new HashMap<>();
      map.put("complex_delimiter_level_1", "#");
      CarbonWriter writer = CarbonWriter.builder()
          .outputPath(path)
          .withLoadOptions(map)
          .withCsvInput(new Schema(fields))
          .writtenBy("CarbonReaderTest")
          .build();

      for (int i = 0; i < 10; i++) {
        String[] row2 = new String[]{
            "robot" + (i % 10),
            String.valueOf(i % 10000),
            String.valueOf(i),
            String.valueOf(Long.MAX_VALUE - i),
            String.valueOf((double) i / 2),
            String.valueOf(true),
            "2019-03-02",
            "2019-02-12 03:03:34",
            "12.345",
            "varchar",
            "Hello#World#From#Carbon",
            "1.23"
        };
        writer.write(row2);
      }
      writer.close();

      File[] dataFiles = new File(path).listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          if (name == null) {
            return false;
          }
          return name.endsWith("carbonindex");
        }
      });
      if (dataFiles == null || dataFiles.length < 1) {
        throw new RuntimeException("Carbon index file not exists.");
      }
      Schema schema = CarbonSchemaReader
          .readSchema(dataFiles[0].getAbsolutePath())
          .asOriginOrder();
      // Transform the schema
      int count = 0;
      for (int i = 0; i < schema.getFields().length; i++) {
        if (!((schema.getFields())[i].getFieldName().contains("."))) {
          count++;
        }
      }
      String[] strings = new String[count];
      int index = 0;
      for (int i = 0; i < schema.getFields().length; i++) {
        if (!((schema.getFields())[i].getFieldName().contains("."))) {
          strings[index] = (schema.getFields())[i].getFieldName();
          index++;
        }
      }
      // Read data
      CarbonReader reader = CarbonReader
          .builder(path, "_temp")
          .projection(strings)
          .build();

      int i = 0;
      while (reader.hasNext()) {
        Object[] data = (Object[]) reader.readNextRow();

        assert (RowUtil.getString(data, 0).equals("robot" + i));
        assertEquals(RowUtil.getShort(data, 1), i);
        assertEquals(RowUtil.getInt(data, 2), i);
        assertEquals(RowUtil.getLong(data, 3), Long.MAX_VALUE - i);
        assertEquals(RowUtil.getDouble(data, 4), ((double) i) / 2);
        assert (RowUtil.getBoolean(data, 5));
        assertEquals(RowUtil.getInt(data, 6), 17957);
        assert (RowUtil.getDecimal(data, 8).equals("12.35"));
        assert (RowUtil.getVarchar(data, 9).equals("varchar"));

        Object[] arr = RowUtil.getArray(data, 10);
        assert (arr[0].equals("Hello"));
        assert (arr[1].equals("World"));
        assert (arr[2].equals("From"));
        assert (arr[3].equals("Carbon"));

        assertEquals(RowUtil.getFloat(data, 11), (float) 1.23);
        i++;
      }
      reader.close();
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

  @Test
  public void testReadNextRowWithProjectionAndRowUtil() {
    String path = "./carbondata";
    try {
      FileUtils.deleteDirectory(new File(path));

      Field[] fields = new Field[12];
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
      Map<String, String> map = new HashMap<>();
      map.put("complex_delimiter_level_1", "#");
      CarbonWriter writer = CarbonWriter.builder()
          .outputPath(path)
          .withLoadOptions(map)
          .withCsvInput(new Schema(fields))
          .writtenBy("CarbonReaderTest")
          .build();

      for (int i = 0; i < 10; i++) {
        String[] row2 = new String[]{
            "robot" + (i % 10),
            String.valueOf(i % 10000),
            String.valueOf(i),
            String.valueOf(Long.MAX_VALUE - i),
            String.valueOf((double) i / 2),
            String.valueOf(true),
            "2019-03-02",
            "2019-02-12 03:03:34",
            "12.345",
            "varchar",
            "Hello#World#From#Carbon",
            "1.23"
        };
        writer.write(row2);
      }
      writer.close();

      // Read data
      CarbonReader reader = CarbonReader
          .builder(path, "_temp")
          .withRowRecordReader()
          .build();

      int i = 0;
      while (reader.hasNext()) {
        Object[] data = (Object[]) reader.readNextRow();

        assert (RowUtil.getString(data, 0).equals("robot" + i));
        assertEquals(RowUtil.getInt(data, 1), 17957);
        assert (RowUtil.getVarchar(data, 3).equals("varchar"));
        Object[] arr = RowUtil.getArray(data, 4);
        assert (arr[0].equals("Hello"));
        assert (arr[1].equals("World"));
        assert (arr[2].equals("From"));
        assert (arr[3].equals("Carbon"));
        assertEquals(RowUtil.getShort(data, 5), i);
        assertEquals(RowUtil.getInt(data, 6), i);
        assertEquals(RowUtil.getLong(data, 7), Long.MAX_VALUE - i);
        assertEquals(RowUtil.getDouble(data, 8), ((double) i) / 2);
        assert (RowUtil.getBoolean(data, 9));
        assert (RowUtil.getDecimal(data, 10).equals("12.35"));
        assertEquals(RowUtil.getFloat(data, 11), (float) 1.23);
        i++;
      }
      assert  (i == 10);
      reader.close();
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

  @Test
  public void testVectorReader() {
    String path = "./testWriteFiles";
    try {
      FileUtils.deleteDirectory(new File(path));

      Field[] fields = new Field[12];
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
      fields[10] = new Field("byteField", DataTypes.BYTE);
      fields[11] = new Field("floatField", DataTypes.FLOAT);
      Map<String, String> map = new HashMap<>();
      map.put("complex_delimiter_level_1", "#");
      CarbonWriter writer = CarbonWriter.builder()
          .outputPath(path)
          .withLoadOptions(map)
          .withCsvInput(new Schema(fields))
          .writtenBy("CarbonReaderTest")
          .build();

      for (int i = 0; i < 10; i++) {
        String[] row2 = new String[]{
            "robot" + (i % 10),
            String.valueOf(i % 10000),
            String.valueOf(i),
            String.valueOf(Long.MAX_VALUE - i),
            String.valueOf((double) i / 2),
            String.valueOf(true),
            "2019-03-02",
            "2019-02-12 03:03:34",
            "12.345",
            "varchar",
            String.valueOf(i),
            "1.23"
        };
        writer.write(row2);
      }
      writer.close();

      // Read data
      CarbonReader reader = CarbonReader
          .builder(path, "_temp")
          .build();

      int i = 0;
      while (reader.hasNext()) {
        Object[] data = (Object[]) reader.readNextRow();

        assert (RowUtil.getString(data, 0).equals("robot" + i));
        assertEquals(RowUtil.getShort(data, 4), i);
        assertEquals(RowUtil.getInt(data, 5), i);
        assert (RowUtil.getLong(data, 6) == Long.MAX_VALUE - i);
        assertEquals(RowUtil.getDouble(data, 7), ((double) i) / 2);
        assert (RowUtil.getBoolean(data, 8));
        assertEquals(RowUtil.getInt(data, 1), 17957);
        assert (RowUtil.getDecimal(data, 9).equals("12.35"));
        assert (RowUtil.getString(data, 3).equals("varchar"));
        assertEquals(RowUtil.getByte(data, 10), new Byte(String.valueOf(i)));
        assertEquals(RowUtil.getFloat(data, 11), new Float("1.23"));
        i++;
      }
      assert(i==10);
      reader.close();
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

  @Test
  public void testReadNextBatchRow() {
    String path = "./carbondata";
    try {
      FileUtils.deleteDirectory(new File(path));

      Field[] fields = new Field[12];
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
      Map<String, String> map = new HashMap<>();
      map.put("complex_delimiter_level_1", "#");
      CarbonWriter writer = CarbonWriter.builder()
          .outputPath(path)
          .withLoadOptions(map)
          .withCsvInput(new Schema(fields))
          .writtenBy("CarbonReaderTest")
          .build();

      for (int i = 0; i < 10; i++) {
        String[] row2 = new String[]{
            "robot" + (i % 10),
            String.valueOf(i % 10000),
            String.valueOf(i),
            String.valueOf(Long.MAX_VALUE - i),
            String.valueOf((double) i / 2),
            String.valueOf(true),
            "2019-03-02",
            "2019-02-12 03:03:34",
            "12.345",
            "varchar",
            "Hello#World#From#Carbon",
            "1.23"
        };
        writer.write(row2);
      }
      writer.close();

      // Read data
      int batchSize =4;
      CarbonReader reader = CarbonReader
          .builder(path, "_temp")
          .withBatch(4)
          .build();

      int i = 0;
      while (reader.hasNext()) {
        Object[] batch = reader.readNextBatchRow();
        Assert.assertTrue(batch.length <= batchSize);

        for (int j = 0; j < batch.length; j++) {

          Object[] data = (Object[]) batch[j];
          assert (RowUtil.getString(data, 0).equals("robot" + i));
          assertEquals(RowUtil.getInt(data, 1), 17957);
          assert (RowUtil.getVarchar(data, 3).equals("varchar"));
          Object[] arr = RowUtil.getArray(data, 4);
          assert (arr[0].equals("Hello"));
          assert (arr[1].equals("World"));
          assert (arr[2].equals("From"));
          assert (arr[3].equals("Carbon"));
          assertEquals(RowUtil.getShort(data, 5), i);
          assertEquals(RowUtil.getInt(data, 6), i);
          assertEquals(RowUtil.getLong(data, 7), Long.MAX_VALUE - i);
          assertEquals(RowUtil.getDouble(data, 8), ((double) i) / 2);
          assert (RowUtil.getBoolean(data, 9));
          assert (RowUtil.getDecimal(data, 10).equals("12.35"));
          assertEquals(RowUtil.getFloat(data, 11), (float) 1.23);
          i++;
        }
        System.out.println("batch is " + i);
      }
      reader.close();
    } catch (Throwable e) {
      e.printStackTrace();
    } finally {
      try {
        FileUtils.deleteDirectory(new File(path));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
  @Test
  public void testReadNextBatchRowWithVectorReader() {
    String path = "./carbondata";
    try {
      FileUtils.deleteDirectory(new File(path));

      Field[] fields = new Field[11];
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
      // Vector don't support complex data type
      // fields[10] = new Field("arrayField", DataTypes.createArrayType(DataTypes.STRING));
      fields[10] = new Field("floatField", DataTypes.FLOAT);
      Map<String, String> map = new HashMap<>();
      map.put("complex_delimiter_level_1", "#");
      CarbonWriter writer = CarbonWriter.builder()
          .outputPath(path)
          .withLoadOptions(map)
          .withCsvInput(new Schema(fields))
          .writtenBy("CarbonReaderTest")
          .build();

      for (int i = 0; i < 10; i++) {
        String[] row2 = new String[]{
            "robot" + (i % 10),
            String.valueOf(i % 10000),
            String.valueOf(i),
            String.valueOf(Long.MAX_VALUE - i),
            String.valueOf((double) i / 2),
            String.valueOf(true),
            "2019-03-02",
            "2019-02-12 03:03:34",
            "12.345",
            "varchar",
            "1.23"
        };
        writer.write(row2);
      }
      writer.close();

      // Read data
      int batchSize =4;
      CarbonReader reader = CarbonReader
          .builder(path, "_temp")
          .withBatch(4)
          .build();

      int i = 0;
      while (reader.hasNext()) {
        Object[] batch = reader.readNextBatchRow();
        Assert.assertTrue(batch.length <= batchSize);

        for (int j = 0; j < batch.length; j++) {

          Object[] data = (Object[]) batch[j];
          assert (RowUtil.getString(data, 0).equals("robot" + i));
          assertEquals(RowUtil.getInt(data, 1), 17957);
          assert (RowUtil.getVarchar(data, 3).equals("varchar"));
          assertEquals(RowUtil.getShort(data, 4), i);
          assertEquals(RowUtil.getInt(data, 5), i);
          assertEquals(RowUtil.getLong(data, 6), Long.MAX_VALUE - i);
          assertEquals(RowUtil.getDouble(data, 7), ((double) i) / 2);
          assert (RowUtil.getDecimal(data, 9).equals("12.35"));
          assertEquals(RowUtil.getFloat(data, 10), (float) 1.23);
          i++;
        }
        System.out.println("batch is " + i);
      }
      reader.close();
    } catch (Throwable e) {
      e.printStackTrace();
    } finally {
      try {
        FileUtils.deleteDirectory(new File(path));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  public void testReadingNullValues() {
    String path = "./testWriteFiles";
    try {
      FileUtils.deleteDirectory(new File(path));

      Field[] fields = new Field[2];
      fields[0] = new Field("stringField", DataTypes.STRING);
      fields[1] = new Field("booleanField", DataTypes.BOOLEAN);
      CarbonWriter writer = CarbonWriter.builder()
          .outputPath(path)
          .withCsvInput(new Schema(fields))
          .writtenBy("CarbonReaderTest")
          .build();

      for (int i = 0; i < 2; i++) {
        String[] row2 = new String[]{
            "robot" + (i % 10),
            "",
        };
        writer.write(row2);
      }
      writer.close();

      // Read data
      CarbonReader reader = CarbonReader
          .builder(path, "_temp")
          .build();

      int i = 0;
      while (reader.hasNext()) {
        reader.readNextRow();
        i++;
      }
      assert (i == 2);
      reader.close();
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
