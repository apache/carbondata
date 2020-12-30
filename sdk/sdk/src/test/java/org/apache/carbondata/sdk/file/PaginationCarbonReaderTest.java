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
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.Field;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.NotEqualsExpression;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test suite for {@link CSVCarbonWriter}
 */
public class PaginationCarbonReaderTest {

  @Test
  public void testMultipleBlocklet() throws IOException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    // create more than one blocklet
    TestUtil.writeFilesAndVerify(1000 * 3000, new Schema(fields), path, null, 1, 2);
    // configure cache size = 8 blocklet
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_MAX_PAGINATION_LRU_CACHE_SIZE_IN_MB, "8");
    try {
      CarbonReaderBuilder carbonReaderBuilder =
          new CarbonReaderBuilder(path, "temptest")
              .withPaginationSupport();
      PaginationCarbonReader<Object> paginationCarbonReader =
          (PaginationCarbonReader<Object>) carbonReaderBuilder.build();
      assert(paginationCarbonReader.getTotalRows() == 3000000);
      Object[] rows;
      // test query random range
      rows = paginationCarbonReader.read(100, 300);
      assert(rows.length == 201);
      rows = paginationCarbonReader.read(21, 1000000);
      assert(rows.length == 999980);
      rows = paginationCarbonReader.read(1000001, 3000000);
      assert(rows.length == 2000000);
      // test case creates 8 blocklets and total rows are split as shown below
      //      0 = [1 - 416000]
      //      1 = [416001 - 832000]
      //      2 = [832001 - 1248000]
      //      3 = [1248001 - 1664000]
      //      4 = [1664001 - 2080000]
      //      5 = [2080001 - 2496000]
      //      6 = [2496001 - 2912000]
      //      7 = [2912001 - 3000000]
      // so test for all combination of ranges
      // 1. from resides in one blocklet, to resides in another blocklet
      // a. from and to exist beside each other (from - 0, to - 1)
      rows = paginationCarbonReader.read(415999, 830000);
      assert(rows.length == 414002);
      // b. from and to exit with some gap (from - 0, to - 3)
      rows = paginationCarbonReader.read(1, 1248005);
      assert(rows.length == 1248005);
      // 2. from and to resides in the same blocklet
      // a. whole blocklet
      rows = paginationCarbonReader.read(2496001, 2912000);
      assert(rows.length == 416000);
      // b. some rows in blocklet
      rows = paginationCarbonReader.read(2912101, 2912301);
      assert(rows.length == 201);
      // read one row
      rows = paginationCarbonReader.read(10, 10);
      assert(rows.length == 1);
      // test negative scenario inputs
      try {
        rows = paginationCarbonReader.read(-1, 2);
        // fail the test if read is successful for invalid arguments
        assert(false);
        rows = paginationCarbonReader.read(1, -2);
        assert(false);
        rows = paginationCarbonReader.read(0, 100);
        assert(false);
        rows = paginationCarbonReader.read(1, 3000001);
        assert(false);
        rows = paginationCarbonReader.read(100, 10);
        assert(false);
      } catch (Exception ex) {
        // nothing to do, expected to throw exception for negative inputs.
      }
      // close the reader
      paginationCarbonReader.close();
      // test read after closing
      try {
        rows = paginationCarbonReader.read(10, 100);
        assert(false);
      } catch (Exception ex) {
        // nothing to do, expected to throw exception for negative scenario.
      }
    } catch (Exception ex) {
      Assert.fail(ex.getMessage());
    }
    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testDataCorrectness() throws IOException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("data", DataTypes.VARCHAR);
    fields[2] = new Field("id", DataTypes.LONG);


    String data = RandomStringUtils.randomAlphabetic(1024);
    // create more than one blocklet
    try {
      CarbonWriterBuilder builder = CarbonWriter.builder()
          .outputPath(path).withBlockletSize(1).withBlockSize(2).withTableProperty("local_dictionary_enable", "false");
      CarbonWriter writer = builder.withCsvInput(new Schema(fields)).writtenBy("TestUtil").build();
      for (int i = 1; i <= 100000; i++) {
        writer.write(new String[]{ "robot" + i, data , String.valueOf(i)});
      }
      writer.close();
    } catch (Exception ex) {
      assert(false);
    }

    // configure cache size = 4 blocklet
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_MAX_PAGINATION_LRU_CACHE_SIZE_IN_MB, "4");
    try {
      CarbonReaderBuilder carbonReaderBuilder =
          new CarbonReaderBuilder(path, "temptest")
              .withPaginationSupport();
      PaginationCarbonReader<Object> paginationCarbonReader =
          (PaginationCarbonReader<Object>) carbonReaderBuilder.build();
      assert(paginationCarbonReader.getTotalRows() == 100000);
      // test case creates 4 blocklets and total rows are split as shown below
      //      0 = 1 - 32000
      //      1 = 32001 - 64000
      //      2 = 64001 - 96000
      //      3 = 96001 - 100000
      Object[] rows;
      // so test for all combination of ranges
      // 1. from resides in one blocklet, to resides in another blocklet
      // a. from and to exist beside each other (from - 0, to - 1)
      rows = paginationCarbonReader.read(31999, 32005);
      assert(rows.length == 7); // note length is (from - to + 1)
      int index = 31999;
      for (Object row : rows) {
        // verify the result
        assert (((Object [])row)[0].equals("robot" + (index)));
        index++;
      }
      // b. from and to exit with some gap (from - 0, to - 3)
      rows = paginationCarbonReader.read(31999, 64005);
      assert(rows.length == 32007); // (from - to + 1)
      index = 31999;
      for (Object row : rows) {
        // verify the result
        assert (((Object [])row)[0].equals("robot" + (index)));
        index++;
      }
      // 2. from and to resides in the same blocklet
      // a. whole blocklet
      rows = paginationCarbonReader.read(64001, 96000);
      assert(rows.length == 32000); // (from - to + 1)
      index = 64001;
      for (Object row : rows) {
        // verify the result
        assert (((Object [])row)[0].equals("robot" + (index)));
        index++;
      }
      // b. some rows in blocklet
      rows = paginationCarbonReader.read(100, 300);
      assert(rows.length == 201); // (from - to + 1)
      index = 100;
      for (Object row : rows) {
        // verify the result
        assert (((Object [])row)[0].equals("robot" + (index)));
        index++;
      }
      // read one row
      rows = paginationCarbonReader.read(10, 10);
      assert(rows.length == 1); // (from - to + 1)
      index = 10;
      for (Object row : rows) {
        // verify the result
        assert (((Object [])row)[0].equals("robot" + (index)));
        index++;
      }
      // close the reader
      paginationCarbonReader.close();
    } catch (Exception ex) {
      Assert.fail(ex.getMessage());
    }
    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testSDKPaginationFilter() throws IOException, InterruptedException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("data", DataTypes.VARCHAR);
    fields[2] = new Field("id", DataTypes.LONG);


    String data = RandomStringUtils.randomAlphabetic(1024);
    // create more than one blocklet
    try {
      CarbonWriterBuilder builder = CarbonWriter.builder()
          .outputPath(path).withBlockletSize(1).withBlockSize(2).withTableProperty("local_dictionary_enable", "false");
      CarbonWriter writer = builder.withCsvInput(new Schema(fields)).writtenBy("TestUtil").build();
      for (int i = 1; i <= 100000; i++) {
        writer.write(new String[]{"robot" + i, data, String.valueOf(i)});
      }
      writer.close();
    } catch (Exception ex) {
      assert (false);
    }

    // configure cache size = 4 blocklet
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_MAX_PAGINATION_LRU_CACHE_SIZE_IN_MB, "4");

    //filter expression
    EqualToExpression equalExpression =
        new EqualToExpression(new ColumnExpression("name", DataTypes.STRING),
            new LiteralExpression("robot1", DataTypes.STRING));

    CarbonReaderBuilder carbonReaderBuilder = CarbonReader.builder(path, "_temp")
        .withPaginationSupport().projection(new String[]{"name", "id"}).filter(equalExpression);

    PaginationCarbonReader<Object> paginationCarbonReader =
        (PaginationCarbonReader<Object>) carbonReaderBuilder.build();
    assert (paginationCarbonReader.getTotalRows() == 1);
    paginationCarbonReader.close();

    // Not Equals expression
    NotEqualsExpression notEqualsExpression =
        new NotEqualsExpression(new ColumnExpression("name", DataTypes.STRING),
            new LiteralExpression("robot1", DataTypes.STRING));
    paginationCarbonReader =
        (PaginationCarbonReader<Object>) carbonReaderBuilder.filter(notEqualsExpression).build();
    assert (paginationCarbonReader.getTotalRows() == 99999);
    paginationCarbonReader.close();
    FileUtils.deleteDirectory(new File(path));
  }

  @Test
  public void testSDKPaginationInsertData() throws IOException, InvalidLoadOptionException, InterruptedException {
    List<String[]> data1 = new ArrayList<String[]>();
    String[] row1 = {"1", "AAA", "3", "3444345.66", "true", "1979-12-09", "2011-2-10 1:00:20", "Pune", "IT"};
    String[] row2 = {"2", "BBB", "2", "543124.66", "false", "1987-2-19", "2017-1-1 12:00:20", "Bangalore", "DATA"};
    String[] row3 = {"3", "CCC", "1", "787878.888", "false", "1982-05-12", "2015-12-1 2:20:20", "Pune", "DATA"};
    String[] row4 = {"4", "DDD", "1", "99999.24", "true", "1981-04-09", "2000-1-15 7:00:20", "Delhi", "MAINS"};
    String[] row5 = {"5", "EEE", "3", "545656.99", "true", "1987-12-09", "2017-11-25 04:00:20", "Delhi", "IT"};

    data1.add(row1);
    data1.add(row2);
    data1.add(row3);
    data1.add(row4);
    data1.add(row5);

    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));
    Field[] fields = new Field[9];
    fields[0] = new Field("id", DataTypes.INT);
    fields[1] = new Field("name", DataTypes.STRING);
    fields[2] = new Field("rank", DataTypes.SHORT);
    fields[3] = new Field("salary", DataTypes.DOUBLE);
    fields[4] = new Field("active", DataTypes.BOOLEAN);
    fields[5] = new Field("dob", DataTypes.DATE);
    fields[6] = new Field("doj", DataTypes.TIMESTAMP);
    fields[7] = new Field("city", DataTypes.STRING);
    fields[8] = new Field("dept", DataTypes.STRING);

    CarbonWriterBuilder builder = CarbonWriter.builder()
        .outputPath(path).withBlockletSize(1).withBlockSize(2);
    CarbonWriter writer = builder.withCsvInput(new Schema(fields)).writtenBy("TestUtil").build();
    for (int i = 0; i < 5; i++) {
      writer.write(data1.get(i));
    }
    writer.close();

    String[] row = {"222", "Daisy", "3", "334.456", "true", "1956-11-08", "2013-12-10 12:00:20", "Pune", "IT"};
    writer = CarbonWriter.builder()
        .outputPath(path).withBlockletSize(1).withBlockSize(2)
        .withCsvInput(new Schema(fields)).writtenBy("TestUtil").build();
    writer.write(row);
    writer.close();

    // configure cache size = 4 blocklet
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_MAX_PAGINATION_LRU_CACHE_SIZE_IN_MB, "4");

    PaginationCarbonReader<Object> paginationCarbonReader =
        (PaginationCarbonReader<Object>) CarbonReader.builder(path, "_temp")
            .withPaginationSupport().projection(new String[]
                {"id", "name", "rank", "salary", "active", "dob", "doj", "city", "dept"}).build();
    assert (paginationCarbonReader.getTotalRows() == 6);
    Object[] rows = paginationCarbonReader.read(1, 6);
    assert (rows.length == 6);

    CarbonIUD.getInstance().delete(path, "name", "AAA").commit();

    CarbonReaderBuilder carbonReaderBuilder = CarbonReader.builder(path, "_temp")
        .withPaginationSupport().projection(new String[]{"id", "name", "rank", "salary", "active", "dob", "doj", "city", "dept"});
    paginationCarbonReader = (PaginationCarbonReader<Object>) carbonReaderBuilder.build();

    assert (paginationCarbonReader.getTotalRows() == 5);
    rows = paginationCarbonReader.read(1, 5);
    assert (rows.length == 5);
    paginationCarbonReader.close();

    CarbonIUD.getInstance().update(path, "name", "AAA", "name", "nihal").commit();
    paginationCarbonReader =
        (PaginationCarbonReader<Object>) CarbonReader.builder(path, "_temp")
            .withPaginationSupport().projection(new String[]
                {"id", "name", "rank", "salary", "active", "dob", "doj", "city", "dept"}).build();
    assert (paginationCarbonReader.getTotalRows() == 5);
    rows = paginationCarbonReader.read(1, 5);
    assert (rows.length == 5);
    paginationCarbonReader.close();
    FileUtils.deleteDirectory(new File(path));
  }
}
