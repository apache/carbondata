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

package org.apache.carbondata.core.cache;

/**
 * interface which declares methods which will decide whether to keep
 * cacheable objects in memory
 */
public interface Cacheable {

  /**
   * This method will return the timestamp of file based on which decision
   * the decision will be taken whether to read that file or not
   *
   * @return
   */
  long getFileTimeStamp();

  /**
   * This method will return the access count for a column based on which decision will be taken
   * whether to keep the object in memory
   *
   * @return
   */
  int getAccessCount();

  /**
   * This method will return the memory size of a column
   *
   * @return
   */
  long getMemorySize();

  /**
   * Method to be used for invalidating the cacheable object. API to be invoked at the time of
   * removing the cacheable object from memory. Example at the of removing the cachebale object
   * from LRU cache
   */
  void invalidate();
}
