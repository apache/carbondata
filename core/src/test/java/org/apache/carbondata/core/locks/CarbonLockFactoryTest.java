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

package org.apache.carbondata.core.locks;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.junit.Assert;
import org.junit.Test;

public class CarbonLockFactoryTest {

  @Test
  public void testCustomLock() {
    CarbonProperties.getInstance().addProperty(
        CarbonCommonConstants.LOCK_TYPE,
        CarbonCommonConstants.CARBON_LOCK_TYPE_CUSTOM
    );
    CarbonProperties.getInstance().addProperty(
        CarbonCommonConstants.LOCK_CLASS,
        CustomLock.class.getName()
    );
    final List<Thread> threadList = new ArrayList<>(2);
    for (int index = 0; index < 2; index++) {
      threadList.add(new Thread() {
        @Override
        public void run() {
          ICarbonLock lock = CarbonLockFactory.getSystemLevelCarbonLockObj("root", "test");
          // do something.
          try {
            Thread.sleep(1500L);
          } catch (InterruptedException ignore) {
            // ignore.
          }
          lock.unlock();
        }
      });
    }
    final long startTime = System.nanoTime();
    for (Thread thread : threadList) {
      thread.start();
    }
    for (Thread thread : threadList) {
      try {
        thread.join();
      } catch (InterruptedException ignore) {
        // ignore.
      }
    }
    Assert.assertTrue(
        TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS) > 3000L
    );
  }

  public static final class CustomLock implements ICarbonLock {

    private static final ConcurrentHashMap<String, ReentrantLock> LOCK_MAP = new ConcurrentHashMap<>();

    private static ReentrantLock getLock(final String lockIdentifier) {
      ReentrantLock lock = LOCK_MAP.get(lockIdentifier);
      if (lock == null) {
        synchronized (LOCK_MAP) {
          lock = LOCK_MAP.get(lockIdentifier);
          if (lock == null) {
            lock = new ReentrantLock();
            LOCK_MAP.put(lockIdentifier, lock);
          }
        }
      }
      return lock;
    }

    public CustomLock(final String absoluteLockPath, final String lockFile) {
      this.lockIdentifier = absoluteLockPath + lockFile;
      getLock(this.lockIdentifier).lock();
    }

    private final String lockIdentifier;

    @Override
    public boolean unlock() {
      getLock(this.lockIdentifier).unlock();
      return true;
    }

    @Override
    public boolean lockWithRetries() {
      // TODO
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean lockWithRetries(int retryCount, int retryInterval) {
      // TODO
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean releaseLockManually(String lockFile) {
      getLock(lockFile).unlock();
      return true;
    }

    @Override
    public String getLockFilePath() {
      return this.lockIdentifier;
    }

  }

}
