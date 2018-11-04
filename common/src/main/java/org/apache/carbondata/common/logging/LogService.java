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

package org.apache.carbondata.common.logging;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.carbondata.common.logging.impl.AuditLevel;
import org.apache.carbondata.common.logging.impl.StatisticLevel;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

/**
 * Log Services, wrapper of org.apache.log4j.Logger
 */
public class LogService extends Logger {

  private static String hostName;
  private static String username;

  {
    try {
      hostName = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      hostName = "localhost";
    }
    try {
      username = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      username = "unknown";
    }
  }

  protected LogService(String name) {
    super(name);
  }

  public void debug(String message) {
    super.debug(message);
  }

  public void info(String message) {
    super.info(message);
  }

  public void warn(String message) {
    super.warn(message);
  }

  public void error(String message) {
    super.error(message);
  }

  public void error(Throwable throwable) {
    super.error(throwable);
  }

  public void error(Throwable throwable, String message) {
    super.error(message, throwable);
  }

  public void audit(String message) {
    String threadid = Thread.currentThread().getId() + "";
    super.log(AuditLevel.AUDIT,
        "[" + hostName + "]" + "[" + username + "]" + "[Thread-" + threadid + "]" + message);
  }

  /**
   * Below method will be used to log the statistic information
   *
   * @param message statistic message
   */
  public void statistic(String message) {
    super.log(StatisticLevel.STATISTIC, message);
  }

  public boolean isDebugEnabled() {
    return super.isDebugEnabled();
  }

  public boolean isWarnEnabled() {
    return super.isEnabledFor(org.apache.log4j.Level.WARN);
  }

  public boolean isInfoEnabled() {
    return super.isInfoEnabled();
  }
}
