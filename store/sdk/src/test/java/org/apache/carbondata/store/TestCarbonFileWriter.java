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

package org.apache.carbondata.store;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.store.api.CarbonFileWriter;
import org.apache.carbondata.store.api.CarbonStore;
import org.apache.carbondata.store.api.SchemaBuilder;
import org.apache.carbondata.store.api.Segment;
import org.apache.carbondata.store.api.Table;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestCarbonFileWriter {

  @Test
  public void testCreateAndWriteTable() throws IOException {
    String tablePath = "./db1/tc1";
    if (new File(tablePath).exists()) {
      new File(tablePath).delete();
    }

    CarbonStore carbon = CarbonStore.build();
    TableSchema schema = SchemaBuilder.newInstance()
        .addColumn(new StructField("name", DataTypes.STRING), true)
        .addColumn(new StructField("age", DataTypes.INT), false)
        .addColumn(new StructField("height", DataTypes.DOUBLE), false)
        .create();

    try {
      Table table = carbon.createTable("t1", schema, tablePath);

      Assert.assertTrue(new File(tablePath).exists());
      Assert.assertTrue(new File(CarbonTablePath.getMetadataPath(tablePath)).exists());

      Segment segment = table.newBatchSegment();
      segment.open();
      CarbonFileWriter writer = segment.newWriter();
      for (int i = 0; i < 1000; i++) {
        writer.writeRow(new String[]{"amy", "1", "2.3"});
      }
      writer.close();
      segment.commit();
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    File segmentFolder = new File(CarbonTablePath.getSegmentPath(tablePath, "0"));
    Assert.assertTrue(segmentFolder.exists());

    File[] dataFiles = segmentFolder.listFiles(new FileFilter() {
      @Override public boolean accept(File pathname) {
        return pathname.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT);
      }
    });
    Assert.assertNotNull(dataFiles);
    Assert.assertEquals(1, dataFiles.length);

    if (new File(tablePath).exists()) {
      FileUtils.deleteDirectory(new File(tablePath));
    }
  }

}
