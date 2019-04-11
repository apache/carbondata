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
package org.apache.carbondata.core.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Carbon thread factory class
 */
public class CarbonThreadFactory implements ThreadFactory {

  /**
   * default thread factory
   */
  private ThreadFactory defaultFactory;

  /**
   * pool name
   */
  private String name;

  private boolean withTime = false;

  public CarbonThreadFactory(String name) {
    this.defaultFactory = Executors.defaultThreadFactory();
    this.name = name;
  }

  public CarbonThreadFactory(String name, boolean withTime) {
    this(name);
    this.withTime = withTime;
  }

  @Override public Thread newThread(Runnable r) {
    final Thread thread = defaultFactory.newThread(r);
    if (withTime) {
      thread.setName(name + "_" + System.currentTimeMillis());
    } else {
      thread.setName(name);
    }
    return thread;
  }
}
