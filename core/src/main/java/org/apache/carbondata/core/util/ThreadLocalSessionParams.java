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

/**
 * This class maintains ThreadLocal session params
 */
public class ThreadLocalSessionParams {
  static final InheritableThreadLocal<SessionParams> threadLocal =
      new InheritableThreadLocal<SessionParams>();

  public static void setSessionParams(SessionParams sessionParams) {
    threadLocal.set(sessionParams);
  }

  public static SessionParams getSessionParams() {
    return threadLocal.get();
  }
}
