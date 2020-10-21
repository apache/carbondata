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

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.constants.CarbonVersionConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.Field;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.sdk.file.*;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class CarbonCliTest {

  private String path = "./CarbonCliTest";
  private String pathBinary = "./CarbonCliTestBinary";

  private String buildLines(String... lines) {
    ByteArrayOutputStream expectedOut = null;
    PrintStream expectedStream = null;
    try {
      expectedOut = new ByteArrayOutputStream();
      expectedStream = new PrintStream(expectedOut);
      for (String line : lines) {
        expectedStream.println(line);
      }

      return new String(expectedOut.toByteArray());
    } finally {
      CarbonUtil.closeStreams(expectedStream, expectedOut);
    }
  }

  @Before
  public void before() throws IOException {
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(5000000, new Schema(fields), path, new String[]{"name", "age"}, 3, 8);
    TestUtil.writeFilesAndVerify(5000000, new Schema(fields), path, new String[]{"name", "age"}, 3, 8);
  }

  public void buildBinaryData(int rows, Schema schema, String path, String[] sortColumns,
      int blockletSize, int blockSize)
      throws IOException, InvalidLoadOptionException {

    CarbonWriterBuilder builder = CarbonWriter.builder()
        .outputPath(path);
    if (sortColumns != null) {
      builder = builder.sortBy(sortColumns);
    }
    if (blockletSize != -1) {
      builder = builder.withBlockletSize(blockletSize);
    }
    if (blockSize != -1) {
      builder = builder.withBlockSize(blockSize);
    }

    CarbonWriter writer = builder.withCsvInput(schema).writtenBy("TestUtil").build();

    for (int i = 0; i < rows; i++) {
      writer.write(new String[]{
          "robot" + (i % 10), String.valueOf(i % 3000000), String.valueOf((double) i / 2)});
    }
    for (int i = 0; i < rows; i++) {
      writer.write(new String[]{
          "robot" + (i % 10), String.valueOf(i % 3000000), String.valueOf("robot" + i / 2)});
    }
    writer.close();
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

    String expectedOutput = buildLines(
        "Input Folder: ./CarbonCliTest" ,
        "## Summary",
        "total: 6 blocks, 2 shards, 10 blocklets, 314 pages, 10,000,000 rows, 24.92MB",
        "avg: 4.15MB/block, 2.49MB/blocklet, 1,666,666 rows/block, 1,000,000 rows/blocklet");
    Assert.assertTrue(output.contains(expectedOutput));

    String[] args2 = {"-cmd", "summary", "-p", path, "-s"};
    out = new ByteArrayOutputStream();
    stream = new PrintStream(out);
    CarbonCli.run(args2, stream);
    output = new String(out.toByteArray());

    expectedOutput = buildLines(
        "Column Name  Data Type  Column Type  SortColumn  Encoding  Ordinal  Id  ",
        "name         STRING     dimension    true        []        0        NA  ",
        "age          INT        dimension    true        []        1        NA  ");
    Assert.assertTrue(output.contains(expectedOutput));

    String[] args3 = {"-cmd", "summary", "-p", path, "-t"};
    out = new ByteArrayOutputStream();
    stream = new PrintStream(out);
    CarbonCli.run(args3, stream);
    output = new String(out.toByteArray());

    expectedOutput = buildLines(
        "## Table Properties",
        "schema file not found");
    Assert.assertTrue(output.contains(expectedOutput));

    String[] args4 = {"-cmd", "summary", "-p", path, "-b", "7"};
    out = new ByteArrayOutputStream();
    stream = new PrintStream(out);
    CarbonCli.run(args4, stream);
    output = new String(out.toByteArray());

    expectedOutput = buildLines(
        "BLK  BLKLT  NumPages  NumRows    Size    " ,
        "0    0      32        1,024,000  2.55MB  " ,
        "0    1      32        1,024,000  2.55MB  " ,
        "1    0      32        1,024,000  2.56MB  " ,
        "1    1      32        1,024,000  2.55MB  " ,
        "2    0      29        904,000    2.26MB  ");
    Assert.assertTrue(output.contains(expectedOutput));

    String[] args5 = {"-cmd", "summary", "-p", path, "-c", "name"};
    out = new ByteArrayOutputStream();
    stream = new PrintStream(out);
    CarbonCli.run(args5, stream);
    output = new String(out.toByteArray());

    expectedOutput = buildLines(
        "BLK  BLKLT  Meta Size  Data Size  LocalDict  DictEntries  DictSize  AvgPageSize  Min%  Max%  Min     Max     " ,
        "0    0      3.67KB     3.89KB     true       3            23.0B     7.0B         NA    NA    robot0  robot2  " ,
        "0    1      3.67KB     3.89KB     true       3            23.0B     7.0B         NA    NA    robot2  robot4  " ,
        "1    0      3.67KB     3.89KB     true       3            23.0B     7.0B         NA    NA    robot4  robot6  " ,
        "1    1      3.67KB     3.89KB     true       3            23.0B     7.0B         NA    NA    robot6  robot8  " ,
        "2    0      3.32KB     3.52KB     true       2            16.0B     7.0B         NA    NA    robot8  robot9  ");
    Assert.assertTrue(output.contains(expectedOutput));
  }

  @Test
  public void testSortColumnsOfSegmentFolder() {
    String[] args = {"-cmd", "sort_columns", "-p", path};
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream stream = new PrintStream(out);
    CarbonCli.run(args, stream);
    String output = new String(out.toByteArray());
    String expectedOutput = buildLines(
        "Input Folder: ./CarbonCliTest",
        "sorted by name,age");
    Assert.assertTrue(output.contains(expectedOutput));
  }

  @Test
  public void testSummaryOutputAll() {
    String[] args = {"-cmd", "summary", "-p", path, "-a", "-c", "age"};
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream stream = new PrintStream(out);
    CarbonCli.run(args, stream);
    String output = new String(out.toByteArray());

    String expectedOutput = buildLines(
        "Input Folder: ./CarbonCliTest" ,
        "## Summary",
        "total: 6 blocks, 2 shards, 10 blocklets, 314 pages, 10,000,000 rows, 24.92MB",
        "avg: 4.15MB/block, 2.49MB/blocklet, 1,666,666 rows/block, 1,000,000 rows/blocklet");

    Assert.assertTrue(output.contains(expectedOutput));

    expectedOutput = buildLines(
        "Column Name  Data Type  Column Type  SortColumn  Encoding  Ordinal  Id  ",
        "name         STRING     dimension    true        []        0        NA  ",
        "age          INT        dimension    true        []        1        NA  ");
    Assert.assertTrue(output.contains(expectedOutput));

    expectedOutput = buildLines(
        "## Table Properties",
        "schema file not found");
    Assert.assertTrue(output.contains(expectedOutput));

    expectedOutput = buildLines(
        "BLK  BLKLT  NumPages  NumRows    Size    ",
        "0    0      32        1,024,000  2.55MB  ",
        "0    1      32        1,024,000  2.55MB  ",
        "1    0      32        1,024,000  2.56MB  ",
        "1    1      32        1,024,000  2.55MB  ");
    Assert.assertTrue(output.contains(expectedOutput));

    expectedOutput = buildLines(
        "BLK  BLKLT  Meta Size  Data Size  LocalDict  DictEntries  DictSize  AvgPageSize  Min%  Max%   Min  Max      " ,
        "0    0      3.49KB     2.55MB     false      0            0.0B      81.39KB      0.0   100.0  0    2999991  " ,
        "0    1      3.50KB     2.54MB     false      0            0.0B      81.30KB      0.0   100.0  3    2999993  " ,
        "1    0      3.50KB     2.55MB     false      0            0.0B      81.54KB      0.0   100.0  5    2999995  " ,
        "1    1      3.50KB     2.54MB     false      0            0.0B      81.30KB      0.0   100.0  7    2999997  " ,
        "2    0      3.17KB     2.25MB     false      0            0.0B      79.48KB      0.0   100.0  9    2999999  " );
    Assert.assertTrue(output.contains(expectedOutput));
    Assert.assertTrue(output.contains("## version Details"));
    Assert.assertTrue(output.contains("written_by  Version"));
    Assert.assertTrue(output.contains("TestUtil    "+ CarbonVersionConstants.CARBONDATA_VERSION));
  }

  @Test
  public void testSummaryPageMeta() {
    String[] args = { "-cmd", "summary", "-p", path, "-c", "name", "-k"};
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream stream = new PrintStream(out);
    CarbonCli.run(args, stream);
    String output = new String(out.toByteArray());
    String expectedOutput = buildLines(
        "Blocklet 0:",
        "Page 0 (offset 0, length 7): DataChunk2(chunk_meta:ChunkCompressionMeta(compression_codec:DEPRECATED, total_uncompressed_size:128000, total_compressed_size:7, compressor_name:snappy), rowMajor:false, data_page_length:3, rle_page_length:4, presence:PresenceMeta(represents_presence:true, present_bit_stream:00), sort_state:SORT_NATIVE, encoders:[ADAPTIVE_INTEGRAL, DICTIONARY, RLE], encoder_meta:[00 04 6E 61 6D 65 05 04 FF FF FF FF FF FF FF FF 0E 00 00 00 00 00 00 00 00 02 00 00 00 02 00 00 00 00 00 00 00 00 00 06 73 6E 61 70 70 79], min_max:BlockletMinMaxIndex(min_values:[72 6F 62 6F 74 30], max_values:[72 6F 62 6F 74 30], min_max_presence:[true]), numberOfRowsInpage:32000)");
    Assert.assertTrue(output.contains(expectedOutput));
  }

  @Test
  public void testSummaryAllColumns() {
    String[] args = { "-cmd", "summary", "-p", path, "-C" };
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream stream = new PrintStream(out);
    CarbonCli.run(args, stream);
    String output = new String(out.toByteArray());
    Assert.assertTrue(output.contains("Block  Blocklet  Column Name  Meta Size  Data Size"));
  }

  @Test
  public void testSummaryAllColumnsForOneFile() {
    CarbonFile folder = FileFactory.getCarbonFile(path);
    CarbonFile[] carbonFiles = folder.listFiles(new CarbonFileFilter() {

      @Override
      public boolean accept(CarbonFile file) {
        return file.getName().endsWith(CarbonTablePath.CARBON_DATA_EXT);
      }
    });

    String[] args = { "-cmd", "summary", "-p", carbonFiles[0].getCanonicalPath(), "-C" };
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream stream = new PrintStream(out);
    CarbonCli.run(args, stream);
    String output = new String(out.toByteArray());
    Assert.assertTrue(output.contains("Block  Blocklet  Column Name  Meta Size  Data Size"));
  }

  @Test
  public void testBenchmark() {
    String[] args = {"-cmd", "benchmark", "-p", path, "-a", "-c", "name"};
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream stream = new PrintStream(out);
    CarbonCli.run(args, stream);
  }

  @Test
  public void testBinary() throws IOException, InvalidLoadOptionException {
    FileUtils.deleteDirectory(new File(pathBinary));
    Field[] fields = new Field[3];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);
    fields[2] = new Field("binaryField", DataTypes.BINARY);

    buildBinaryData(5000000, new Schema(fields), pathBinary, new String[]{"name"}, 3, 8);
    String[] args = {"-cmd", "summary", "-p", pathBinary};
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream stream = new PrintStream(out);
    CarbonCli.run(args, stream);

    String[] args2 = {"-cmd", "summary", "-p", pathBinary, "-s"};
    out = new ByteArrayOutputStream();
    stream = new PrintStream(out);
    CarbonCli.run(args2, stream);
    String output = new String(out.toByteArray());

    Assert.assertTrue(output.contains("binaryfield") && output.contains("BINARY"));
    FileUtils.deleteDirectory(new File(pathBinary));
  }


  @After
  public void after() throws IOException {
    FileUtils.deleteDirectory(new File(path));
    FileUtils.deleteDirectory(new File(pathBinary));
  }

}
