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
package org.apache.carbondata.processing.newflow.sort;

import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.sort.impl.ThreadStatusObserver;

/**
 * The class defines the common methods used in across various type of sort
 */
public abstract class AbstractMergeSorter implements Sorter {
  /**
   * instance of thread status observer
   */
  protected ThreadStatusObserver threadStatusObserver;

  /**
   * Below method will be used to check error in exception
   */
  public void checkError() {
    if (threadStatusObserver.getThrowable() != null) {
      if (threadStatusObserver.getThrowable() instanceof CarbonDataLoadingException) {
        throw (CarbonDataLoadingException) threadStatusObserver.getThrowable();
      } else {
        throw new CarbonDataLoadingException(threadStatusObserver.getThrowable());
      }
    }
  }
}
