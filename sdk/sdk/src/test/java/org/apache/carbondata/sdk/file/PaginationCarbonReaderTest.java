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

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.Field;
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

}
