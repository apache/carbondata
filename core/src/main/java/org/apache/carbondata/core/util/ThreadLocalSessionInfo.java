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

import org.apache.hadoop.conf.Configuration;

/**
 * This class maintains ThreadLocal session params
 */
public class ThreadLocalSessionInfo {
  static final InheritableThreadLocal<CarbonSessionInfo> threadLocal =
      new InheritableThreadLocal<CarbonSessionInfo>();

  public static void setCarbonSessionInfo(CarbonSessionInfo carbonSessionInfo) {
    threadLocal.set(carbonSessionInfo);
  }

  public static CarbonSessionInfo getCarbonSessionInfo() {
    return threadLocal.get();
  }

  public static synchronized CarbonSessionInfo getOrCreateCarbonSessionInfo() {
    CarbonSessionInfo info = threadLocal.get();
    if (info == null || info.getSessionParams() == null) {
      info = new CarbonSessionInfo();
      info.setSessionParams(new SessionParams());
      threadLocal.set(info);
    }
    return info;
  }

  public static void setConfigurationToCurrentThread(Configuration configuration) {
    getOrCreateCarbonSessionInfo().getNonSerializableExtraInfo().put("carbonConf", configuration);
  }

  public static void unsetAll() {
    threadLocal.remove();
  }

  /**
   * use to check whether the thread has the hadoop configuration or not
   */
  public static void shouldHaveCarbonConf() {
    CarbonSessionInfo sessionInfo = getCarbonSessionInfo();
    if (sessionInfo == null) {
      throw new RuntimeException("the thread not contain CarbonSessionInfo");
    }
    Object conf = sessionInfo.getNonSerializableExtraInfo().get("carbonConf");
    if (conf == null) {
      throw new RuntimeException("the thread not contain hadoop configuration");
    }
  }
}
