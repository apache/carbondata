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
import java.lang.reflect.Field;
import java.util.UUID;

import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.ICarbonLock;
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

  private  String rootPath;

  private Class<?> secretClass = CarbonLockFactory.class;

  /**
   * @throws java.lang.Exception
   */
  @Before public void setUp() throws Exception {
    rootPath = new File(this.getClass().getResource("/").getPath()
        + "../../..").getCanonicalPath();
    String storeLocation = rootPath + "/target/store_new/";
    CarbonProperties.getInstance()
        .addProperty("carbon.storelocation", storeLocation);
  }

  /**
   * @throws java.lang.Exception
   */
  @After public void tearDown() throws Exception {
    Field f = secretClass.getDeclaredField("lockPath");
    f.setAccessible(true);
    f.set(secretClass, "");
    String storeLocation = rootPath + "/target/store_new/";
    new File(storeLocation).delete();
  }

  @Test public void testingLocalFileLockingByAcquiring2Locks() {

    AbsoluteTableIdentifier absoluteTableIdentifier = AbsoluteTableIdentifier
        .from(CarbonProperties.getInstance().getProperty("carbon.storelocation"), "databaseName",
            "tableName", UUID.randomUUID().toString());
    LocalFileLock localLock1 =
        new LocalFileLock(absoluteTableIdentifier.getTablePath(),
            LockUsage.METADATA_LOCK);
    Assert.assertTrue(localLock1.lock());
    LocalFileLock localLock2 =
        new LocalFileLock(absoluteTableIdentifier.getTablePath(),
            LockUsage.METADATA_LOCK);
    Assert.assertTrue(!localLock2.lock());

    Assert.assertTrue(localLock1.unlock());
    Assert.assertTrue(localLock2.lock());
    Assert.assertTrue(localLock2.unlock());
  }

  @Test public void testConfigurablePathForLock() throws Exception {
    try {
      Field f = secretClass.getDeclaredField("lockPath");
      f.setAccessible(true);
      f.set(secretClass, rootPath + "/target/");
      AbsoluteTableIdentifier absoluteTableIdentifier = AbsoluteTableIdentifier
          .from(CarbonProperties.getInstance().getProperty("carbon.storelocation"), "databaseName",
              "tableName1", "1");
      ICarbonLock carbonLock = CarbonLockFactory.getCarbonLockObj(absoluteTableIdentifier, LockUsage.TABLE_STATUS_LOCK);
      carbonLock.lockWithRetries();
      assert (new File(rootPath + "/target/1/LockFiles/tablestatus.lock").exists());
      assert (!new File(absoluteTableIdentifier.getTablePath() + "/LockFiles").exists());
    } finally {
      tearDown();
    }
  }

}
