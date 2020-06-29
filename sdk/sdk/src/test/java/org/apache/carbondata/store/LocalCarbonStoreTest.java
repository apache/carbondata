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
import java.io.IOException;
import java.util.Iterator;

import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.Field;
import org.apache.carbondata.sdk.file.Schema;
import org.apache.carbondata.sdk.file.TestUtil;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

public class LocalCarbonStoreTest {

  // TODO: complete this testcase
  // Currently result rows are empty, because SDK is not writing table status file
  // so that reader does not find any segment.
  // Complete this testcase after flat folder reader is done.
  @Test
  public void testWriteAndReadFiles() throws IOException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    TestUtil.writeFilesAndVerify(100, new Schema(fields), path);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2961

    CarbonStore store = new LocalCarbonStore();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2754
    Iterator<CarbonRow> rows =
        store.scan(AbsoluteTableIdentifier.from(path, "", ""), new String[] { "name, age" }, null);

    while (rows.hasNext()) {
      CarbonRow row = rows.next();
      System.out.println(row.toString());
    }

    FileUtils.deleteDirectory(new File(path));
  }

}
