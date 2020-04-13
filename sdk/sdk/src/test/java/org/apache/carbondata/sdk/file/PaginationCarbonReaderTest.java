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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalType;
import org.apache.carbondata.core.metadata.datatype.Field;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.schema.SchemaReader;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.reader.CarbonFooterReaderV3;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.FileFooter3;
import org.apache.carbondata.hadoop.CarbonInputSplit;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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
    try {
      CarbonReaderBuilder carbonReaderBuilder =
          new CarbonReaderBuilder(path, "temptest")
              .withPaginationSupport();
      PaginationCarbonReader<Object> paginationCarbonReader =
          (PaginationCarbonReader<Object>) carbonReaderBuilder.build();
      assert(paginationCarbonReader.getTotalRows() == 3000000);
      Object[] rows;
      rows = paginationCarbonReader.read(100, 300);
      assert(rows.length == 201);
      rows = paginationCarbonReader.read(21, 1000000);
      assert(rows.length == 999980);
      rows = paginationCarbonReader.read(1000001, 3000000);
      assert(rows.length == 2000000);
      paginationCarbonReader.close();
    } catch (Exception ex) {
      Assert.fail(ex.getMessage());
    }
    FileUtils.deleteDirectory(new File(path));
  }

  // Add validation negative test cases


}
