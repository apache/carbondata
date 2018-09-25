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

package org.apache.carbondata.tool;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.sdk.file.Field;
import org.apache.carbondata.sdk.file.Schema;
import org.apache.carbondata.sdk.file.TestUtil;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CarbonCliTest {

  private String path = "./CarbonCliTest";

  @Before
  public void before() throws IOException {
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(5000000, new Schema(fields), path, new String[]{"name"}, 3, 8);
    TestUtil.writeFilesAndVerify(5000000, new Schema(fields), path, new String[]{"name"}, 3, 8);
  }

  @Test
  public void testInvalidCmd() {
    String[] args = {"-cmd", "DD", "-p", path};
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream stream = new PrintStream(out);
    CarbonCli.run(args, stream);
    String output = new String(out.toByteArray());
    Assert.assertTrue(output.contains("command DD is not supported"));

    String[] args2 = {"-p", path};
    out = new ByteArrayOutputStream();
    stream = new PrintStream(out);
    CarbonCli.run(args2, stream);
    output = new String(out.toByteArray());
    Assert.assertTrue(output.contains("Parsing failed. Reason: Missing required option: cmd"));
  }

  @Test
  public void testSummaryOutputIndividual() {
    String[] args = {"-cmd", "summary", "-p", path};
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream stream = new PrintStream(out);
    CarbonCli.run(args, stream);
    String output = new String(out.toByteArray());
    Assert.assertTrue(
        output.contains(
            "Input Folder: ./CarbonCliTest\n"
          + "## Summary\n"
          + "total: 6 blocks, 2 shards, 14 blocklets, 314 pages, 10,000,000 rows, 32.26MB\n"
          + "avg: 5.38MB/block, 2.30MB/blocklet, 1,666,666 rows/block, 714,285 rows/blocklet"));

    String[] args2 = {"-cmd", "summary", "-p", path, "-s"};
    out = new ByteArrayOutputStream();
    stream = new PrintStream(out);
    CarbonCli.run(args2, stream);
    output = new String(out.toByteArray());
    Assert.assertTrue(
        output.contains(
            "Column Name  Data Type  Column Type  SortColumn  Encoding          Ordinal  Id  \n"
          + "name         STRING     dimension    true        [INVERTED_INDEX]  0        NA  \n"
          + "age          INT        measure      false       []                1        NA  "));

    String[] args3 = {"-cmd", "summary", "-p", path, "-t"};
    out = new ByteArrayOutputStream();
    stream = new PrintStream(out);
    CarbonCli.run(args3, stream);
    output = new String(out.toByteArray());

    Assert.assertTrue(
        output.contains(
            "## Table Properties\n"
          + "schema file not found"));

    String[] args4 = {"-cmd", "summary", "-p", path, "-b"};
    out = new ByteArrayOutputStream();
    stream = new PrintStream(out);
    CarbonCli.run(args4, stream);
    output = new String(out.toByteArray());
    Assert.assertTrue(
        output.contains(
            "BLK  BLKLT  NumPages  NumRows  Size      \n"
          + "0    0      25        800,000  2.58MB    \n"
          + "0    1      25        800,000  2.58MB    \n"
          + "1    0      25        800,000  2.58MB    \n"
          + "1    1      25        800,000  2.58MB    \n"
          + "2    0      25        800,000  2.58MB    \n"
          + "2    1      25        800,000  2.58MB    \n"
          + "2    2      7         200,000  660.74KB  "));

    String[] args5 = {"-cmd", "summary", "-p", path, "-c", "name"};
    out = new ByteArrayOutputStream();
    stream = new PrintStream(out);
    CarbonCli.run(args5, stream);
    output = new String(out.toByteArray());
    Assert.assertTrue(
        output.contains(
            "BLK  BLKLT  Meta Size  Data Size  LocalDict  DictEntries  DictSize  AvgPageSize  Min%    Max%    \n"
          + "0    0      1.72KB     295.89KB   false      0            0.0B      11.77KB      robot0  robot1  \n"
          + "0    1      1.72KB     295.89KB   false      0            0.0B      11.77KB      robot1  robot3  \n"
          + "1    0      1.72KB     295.89KB   false      0            0.0B      11.77KB      robot3  robot4  \n"
          + "1    1      1.72KB     295.89KB   false      0            0.0B      11.77KB      robot4  robot6  \n"
          + "2    0      1.72KB     295.89KB   false      0            0.0B      11.77KB      robot6  robot7  \n"
          + "2    1      1.72KB     295.89KB   false      0            0.0B      11.77KB      robot8  robot9  \n"
          + "2    2      492.0B     74.03KB    false      0            0.0B      10.51KB      robot9  robot9  "));
  }

  @Test
  public void testSummaryOutputAll() {
    String[] args = {"-cmd", "summary", "-p", path, "-a", "-c", "age"};
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream stream = new PrintStream(out);
    CarbonCli.run(args, stream);
    String output = new String(out.toByteArray());
    Assert.assertTrue(
        output.contains(
            "Input Folder: ./CarbonCliTest\n"
          + "## Summary\n"
          + "total: 6 blocks, 2 shards, 14 blocklets, 314 pages, 10,000,000 rows, 32.26MB\n"
          + "avg: 5.38MB/block, 2.30MB/blocklet, 1,666,666 rows/block, 714,285 rows/blocklet\n"));

    Assert.assertTrue(
        output.contains(
            "Column Name  Data Type  Column Type  SortColumn  Encoding          Ordinal  Id  \n"
          + "name         STRING     dimension    true        [INVERTED_INDEX]  0        NA  \n"
          + "age          INT        measure      false       []                1        NA  \n"));

    Assert.assertTrue(
        output.contains(
            "## Table Properties\n"
          + "schema file not found"));

    Assert.assertTrue(
        output.contains(
            "BLK  BLKLT  NumPages  NumRows  Size      \n"
          + "0    0      25        800,000  2.58MB    \n"
          + "0    1      25        800,000  2.58MB    \n"
          + "1    0      25        800,000  2.58MB    \n"
          + "1    1      25        800,000  2.58MB    \n"
          + "2    0      25        800,000  2.58MB    \n"
          + "2    1      25        800,000  2.58MB    \n"
          + "2    2      7         200,000  660.74KB  "));

    Assert.assertTrue(
        output.contains(
          "BLK  BLKLT  Meta Size  Data Size  LocalDict  DictEntries  DictSize  AvgPageSize  Min%  Max%   \n"
        + "0    0      2.90KB     4.87MB     false      0            0.0B      93.76KB      0.0   100.0  \n"
        + "0    1      2.90KB     2.29MB     false      0            0.0B      93.76KB      0.0   100.0  \n"
        + "1    0      2.90KB     4.87MB     false      0            0.0B      93.76KB      0.0   100.0  \n"
        + "1    1      2.90KB     2.29MB     false      0            0.0B      93.76KB      0.0   100.0  \n"
        + "2    0      2.90KB     5.52MB     false      0            0.0B      93.76KB      0.0   100.0  \n"
        + "2    1      2.90KB     2.94MB     false      0            0.0B      93.76KB      0.0   100.0  \n"
        + "2    2      830.0B     586.81KB   false      0            0.0B      83.71KB      0.0   100.0 "));
  }

  @Test
  public void testBenchmark() {
    String[] args = {"-cmd", "benchmark", "-p", path, "-a", "-c", "name"};
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream stream = new PrintStream(out);
    CarbonCli.run(args, stream);
    String output = new String(out.toByteArray());
    System.out.println(output);
  }

  @After
  public void after() throws IOException {
    FileUtils.deleteDirectory(new File(path));
  }

}
