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

/**
 * Carbon Lock Interface which handles the locking and unlocking.
 */
public interface ICarbonLock {

  /**
   * Does the unlocking of the acquired lock.
   *
   * @return
   */
  boolean unlock();

  /**
   * This will acquire the lock and if it doesnt get then it will retry after the confiured time.
   *
   * @return
   */
  boolean lockWithRetries();

  /**
   * This will acquire the lock and if it doesnt get then it will retry after retryInterval.
   *
   * @return
   */
  boolean lockWithRetries(int retryCount, int retryInterval);

  /**
   * This method will delete the lock file at the specified location.
   *
   * @param lockFile The path of the lock file.
   * @return True if the lock file is deleted, false otherwise.
   */
  boolean releaseLockManually(String lockFile);

  /**
   * Return the path to the lock file
   * @return lock file path
   */
  String getLockFilePath();
}
