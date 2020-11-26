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
package org.apache.carbondata.hive;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Arrays;

import org.apache.carbondata.hive.test.server.HiveEmbeddedServer2;

import org.junit.Assert;

/**
 * A utility class to start and stop the Hive Embedded Server.
 */
public abstract class HiveTestUtils {

  protected static Connection connection;

  protected static HiveEmbeddedServer2 hiveEmbeddedServer2;

  public HiveTestUtils() {
  }

  static {
    try {
      File rootPath = new File(HiveTestUtils.class.getResource("/").getPath() + "../../../..");
      String targetLoc = rootPath.getAbsolutePath() + "/integration/hive/target/warehouse";
      String metadatadbLoc = rootPath.getAbsolutePath() + "/integration/hive/target/metastore_db";
      File file = new File(metadatadbLoc);
      if (file.exists()) {
        file.delete();
      }
      hiveEmbeddedServer2 = new HiveEmbeddedServer2();
      hiveEmbeddedServer2.start(targetLoc);
      int port = hiveEmbeddedServer2.getFreePort();
      connection = DriverManager.getConnection("jdbc:hive2://localhost:" + port + "/default", "", "");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public boolean checkMapKeyPairsAfterSorting(String a, String b) {
    String[] actual = a.substring(1, a.length() - 1).split(",");
    String[] expected = b.substring(1, a.length() - 1).split(",");
    Arrays.sort(actual);Arrays.sort(expected);
    return Arrays.equals(actual, expected);
  }

  public boolean checkAnswer(ResultSet actual, ResultSet expected) throws SQLException {
    Assert.assertEquals("Row Count Mismatch: ", expected.getFetchSize(), actual.getFetchSize());
    List<String> expectedValuesList = new ArrayList<>();
    List<String> actualValuesList = new ArrayList<>();
    while (expected.next()) {
      if (!actual.next()) {
        return false;
      }
      int numOfColumnsExpected = expected.getMetaData().getColumnCount();
      Assert.assertTrue(numOfColumnsExpected > 0);
      Assert.assertEquals(actual.getMetaData().getColumnCount(), numOfColumnsExpected);
      for (int i = 1; i <= numOfColumnsExpected; i++) {
        if (actual.getString(i).contains(":")) {
          Assert.assertTrue(checkMapKeyPairsAfterSorting(actual.getString(i), expected.getString(i)));
        } else {
          expectedValuesList.add(expected.getString(i));
          actualValuesList.add(actual.getString(i));
        }
      }
    }
    Collections.sort(expectedValuesList);Collections.sort(actualValuesList);
    Assert.assertArrayEquals(expectedValuesList.toArray(), actualValuesList.toArray());
    return true;
  }

}
