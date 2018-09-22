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

//    TestUtil.writeFilesAndVerify(5000000, new Schema(fields), path, new String[]{"name"},
//        true, 3, 8, true);
//    TestUtil.writeFilesAndVerify(5000000, new Schema(fields), path, new String[]{"name"},
//        true, 3, 8, true);
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
          + "total: 6 blocks, 2 shards, 12 blocklets, 314 pages, 10,000,000 rows, 30.72MB\n"
          + "avg: 5.12MB/block, 2.56MB/blocklet, 1,666,666 rows/block, 833,333 rows/blocklet"));

    String[] args2 = {"-cmd", "summary", "-p", path, "-s"};
    out = new ByteArrayOutputStream();
    stream = new PrintStream(out);
    CarbonCli.run(args2, stream);
    output = new String(out.toByteArray());

    Assert.assertTrue(
        output.contains(
            "Column Name  Data Type  Column Type  SortColumn  Encoding          Ordinal  Id  \n"
          + "age          INT        dimension    true        [INVERTED_INDEX]  1        NA  \n"
          + "name         STRING     dimension    false       [INVERTED_INDEX]  0        NA  \n"));

    String[] args3 = {"-cmd", "summary", "-p", path, "-t"};
    out = new ByteArrayOutputStream();
    stream = new PrintStream(out);
    CarbonCli.run(args3, stream);
    output = new String(out.toByteArray());

    Assert.assertTrue(
        output.contains(
            "## Table Properties\n"
          + "Property Name              Property Value  \n"
          + "'table_blocksize'          '8'             \n"
          + "'table_blocklet_size'      '3'             \n"
          + "'local_dictionary_enable'  'false'    "));

    String[] args4 = {"-cmd", "summary", "-p", path, "-b"};
    out = new ByteArrayOutputStream();
    stream = new PrintStream(out);
    CarbonCli.run(args4, stream);
    output = new String(out.toByteArray());

    Assert.assertTrue(
        output.contains(
            "BLK  BLKLT  NumPages  NumRows  Size    \n"
          + "0    0      29        928,000  2.60MB  \n"
          + "0    1      29        928,000  2.60MB  \n"
          + "1    0      29        928,000  2.60MB  \n"
          + "1    1      29        928,000  2.60MB  \n"
          + "2    0      22        704,000  2.54MB  \n"
          + "2    1      19        584,000  2.43MB  "));

    String[] args5 = {"-cmd", "summary", "-p", path, "-c", "name"};
    out = new ByteArrayOutputStream();
    stream = new PrintStream(out);
    CarbonCli.run(args5, stream);
    output = new String(out.toByteArray());

    Assert.assertTrue(
        output.contains(
            "BLK  BLKLT  Meta Size  Data Size  LocalDict  DictEntries  DictSize  AvgPageSize  Min%    Max%    \n"
          + "0    0      1.82KB     5.19MB     false      0            0.0B      11.96KB      robot0  robot9  \n"
          + "0    1      1.82KB     2.60MB     false      0            0.0B      11.96KB      robot0  robot9  \n"
          + "1    0      1.82KB     5.19MB     false      0            0.0B      11.96KB      robot0  robot9  \n"
          + "1    1      1.82KB     2.60MB     false      0            0.0B      11.96KB      robot0  robot9  \n"
          + "2    0      1.38KB     4.97MB     false      0            0.0B      11.92KB      robot0  robot9  \n"
          + "2    1      1.19KB     2.43MB     false      0            0.0B      11.42KB      robot0  robot9  \n"));
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
          + "total: 6 blocks, 2 shards, 12 blocklets, 314 pages, 10,000,000 rows, 30.72MB\n"
          + "avg: 5.12MB/block, 2.56MB/blocklet, 1,666,666 rows/block, 833,333 rows/blocklet"));

    Assert.assertTrue(
        output.contains(
            "Column Name  Data Type  Column Type  SortColumn  Encoding          Ordinal  Id  \n"
          + "age          INT        dimension    true        [INVERTED_INDEX]  1        NA  \n"
          + "name         STRING     dimension    false       [INVERTED_INDEX]  0        NA  \n"));

    Assert.assertTrue(
        output.contains(
            "## Table Properties\n"
          + "Property Name              Property Value  \n"
          + "'table_blocksize'          '8'             \n"
          + "'table_blocklet_size'      '3'             \n"
          + "'local_dictionary_enable'  'false'    "));

    Assert.assertTrue(
        output.contains(
            "BLK  BLKLT  NumPages  NumRows  Size    \n"
          + "0    0      29        928,000  2.60MB  \n"
          + "0    1      29        928,000  2.60MB  \n"
          + "1    0      29        928,000  2.60MB  \n"
          + "1    1      29        928,000  2.60MB  \n"
          + "2    0      22        704,000  2.54MB  \n"
          + "2    1      19        584,000  2.43MB  "));

    Assert.assertTrue(
        output.contains(
          "BLK  BLKLT  Meta Size  Data Size  LocalDict  DictEntries  DictSize  AvgPageSize  Min%  Max%   \n"
        + "0    0      1.81KB     2.26MB     false      0            0.0B      79.61KB      0.0   15.5   \n"
        + "0    1      1.81KB     2.26MB     false      0            0.0B      79.60KB      15.5  30.9   \n"
        + "1    0      1.81KB     2.26MB     false      0            0.0B      79.62KB      30.9  46.4   \n"
        + "1    1      1.81KB     2.26MB     false      0            0.0B      79.60KB      46.4  61.9   \n"
        + "2    0      1.37KB     2.28MB     false      0            0.0B      106.11KB     61.9  80.5   \n"
        + "2    1      1.19KB     2.22MB     false      0            0.0B      119.55KB     80.5  100.0  "));
  }

  @Test
  public void testBenchmark() {
    String path = "/Users/jacky/code/spark-2.2.1-bin-hadoop2.7/carbonstore/tpchcarbon_base/lineitem";
    String[] args = {"-cmd", "summary", "-p", path, "-a", "-c", "l_orderkey"};
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream stream = new PrintStream(out);
    CarbonCli.run(args, stream);
    String output = new String(out.toByteArray());
    System.out.println(output);

    args = new String[]{"-cmd", "benchmark", "-p", path, "-c", "l_orderkey"};
    out = new ByteArrayOutputStream();
    stream = new PrintStream(out);
    CarbonCli.run(args, stream);
    output = new String(out.toByteArray());
    System.out.println(output);
  }

  @After
  public void after() throws IOException {
    FileUtils.deleteDirectory(new File(path));
  }

}
