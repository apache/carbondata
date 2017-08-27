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

package org.apache.carbondata.processing.loading.sort.impl;

import java.util.concurrent.ExecutorService;

public class ThreadStatusObserver {

  /**
   * lock object
   */
  private Object lock = new Object();

  private ExecutorService executorService;

  private Throwable throwable;

  public ThreadStatusObserver(ExecutorService executorService) {
    this.executorService = executorService;
  }

  public void notifyFailed(Throwable throwable) {
    // Only the first failing thread should call for shutting down the executor service and
    // should assign the throwable object else the actual cause for failure can be overridden as
    // all the running threads will throw interrupted exception on calling shutdownNow and
    // will override the throwable object
    synchronized (lock) {
      if (null == this.throwable) {
        executorService.shutdownNow();
        this.throwable = throwable;
      }
    }
  }

  public Throwable getThrowable() {

    synchronized (lock) {
      return throwable;
    }
  }
}
