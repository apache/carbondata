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
 * Class to keep all the thread local variable for task
 */
public class ThreadLocalTaskInfo {
  static final InheritableThreadLocal<CarbonTaskInfo> threadLocal =
      new InheritableThreadLocal<CarbonTaskInfo>();

  public static void setCarbonTaskInfo(CarbonTaskInfo carbonTaskInfo) {
    threadLocal.set(carbonTaskInfo);
  }

  public static CarbonTaskInfo getCarbonTaskInfo() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1351
    if (null == threadLocal.get()) {
      CarbonTaskInfo carbonTaskInfo = new CarbonTaskInfo();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2991
      carbonTaskInfo.setTaskId(CarbonUtil.generateUUID());
      ThreadLocalTaskInfo.setCarbonTaskInfo(carbonTaskInfo);
    }
    return threadLocal.get();
  }

  public static void clearCarbonTaskInfo() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3080
    if (null != threadLocal.get()) {
      threadLocal.set(null);
    }

  }
}
