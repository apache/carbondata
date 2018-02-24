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
package org.apache.carbondata.lcm.locks;

import java.io.File;

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.locks.LocalFileLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.util.CarbonProperties;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class to test the functionality of the local file locking.
 */
public class LocalFileLockTest {

  /**
   * @throws java.lang.Exception
   */
  @Before public void setUp() throws Exception {
    String rootPath = new File(this.getClass().getResource("/").getPath()
        + "../../..").getCanonicalPath();
    String storeLocation = rootPath + "/target/store";
    CarbonProperties.getInstance()
        .addProperty("carbon.storelocation", storeLocation);
  }

  /**
   * @throws java.lang.Exception
   */
  @After public void tearDown() throws Exception {
  }

  @Test public void testingLocalFileLockingByAcquiring2Locks() {

    AbsoluteTableIdentifier absoluteTableIdentifier = AbsoluteTableIdentifier
        .from(CarbonProperties.getInstance().getProperty("carbon.storelocation"), "databaseName",
            "tableName");
    LocalFileLock localLock1 =
        new LocalFileLock(absoluteTableIdentifier,
            LockUsage.METADATA_LOCK);
    Assert.assertTrue(localLock1.lock());
    LocalFileLock localLock2 =
        new LocalFileLock(absoluteTableIdentifier,
            LockUsage.METADATA_LOCK);
    Assert.assertTrue(!localLock2.lock());

    Assert.assertTrue(localLock1.unlock());
    Assert.assertTrue(localLock2.lock());
    Assert.assertTrue(localLock2.unlock());
  }

}
