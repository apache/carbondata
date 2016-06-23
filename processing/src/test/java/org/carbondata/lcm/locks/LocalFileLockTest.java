/**
 *
 */
package org.carbondata.lcm.locks;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.util.Assert;

/**
 * Test class to test the functionality of the local file locking.
 *
 * @author Administrator
 */
public class LocalFileLockTest {

  /**
   * @throws java.lang.Exception
   */
  @Before public void setUp() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @After public void tearDown() throws Exception {
  }

  @Test public void testingLocalFileLockingByAcquiring2Locks() {

    LocalFileLock localLock1 =
        new LocalFileLock((new File(".").getAbsolutePath()) + "/src/test/resources",
            LockUsage.METADATA_LOCK);
    Assert.assertTrue(localLock1.lock());
    LocalFileLock localLock2 =
        new LocalFileLock((new File(".").getAbsolutePath()) + "/src/test/resources",
            LockUsage.METADATA_LOCK);
    Assert.assertTrue(!localLock2.lock());

    Assert.assertTrue(localLock1.unlock());
    Assert.assertTrue(localLock2.lock());

  }

}
