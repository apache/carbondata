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

package org.apache.carbondata.common.logging.impl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.carbondata.common.logging.LogService;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

/**
 * Default Implementation of the <code>LogService</code>
 */
public final class StandardLogService implements LogService {

  private static final String PARTITION_ID = "[partitionID:";
  private Logger logger;

  /**
   * Constructor.
   *
   * @param clazzName for which the Logging is required
   */
  public StandardLogService(String clazzName) {
    logger = Logger.getLogger(clazzName);
  }

  public StandardLogService() {
    this("Carbon");
  }

  public static String getPartitionID(String tableName) {
    return tableName.substring(tableName.lastIndexOf('_') + 1, tableName.length());
  }

  public static void setThreadName(String partitionID, String queryID) {
    StringBuilder b = new StringBuilder(PARTITION_ID);
    b.append(partitionID);
    if (null != queryID) {
      b.append(";queryID:");
      b.append(queryID);
    }
    b.append("]");
    Thread.currentThread().setName(getThreadName() + b.toString());
  }

  private static String getThreadName() {
    String name = Thread.currentThread().getName();
    int index = name.indexOf(PARTITION_ID);
    if (index > -1) {
      name = name.substring(0, index);
    } else {
      name = '[' + name + ']';
    }
    return name.trim();
  }

  public boolean isDebugEnabled() {
    return logger.isDebugEnabled();
  }

  public boolean isWarnEnabled() {
    return logger.isEnabledFor(org.apache.log4j.Level.WARN);
  }

  public void debug(String message) {
    if (logger.isDebugEnabled()) {
      logMessage(Level.DEBUG, null, message);
    }
  }

  public void error(String message) {
    logMessage(Level.ERROR, null, message);
  }

  public void error(Throwable throwable, String message) {
    logMessage(Level.ERROR, throwable, message);
  }

  public void error(Throwable throwable) {
    logMessage(Level.ERROR, throwable, "");
  }

  public void info(String message) {
    if (logger.isInfoEnabled()) {
      logMessage(Level.INFO, null, message);
    }
  }

  /**
   * Utility Method to log the the Message.
   */
  private void logMessage(Level logLevel, Throwable throwable, String message) {
    try {
      //Append the partition id and query id if exist
      StringBuilder buff = new StringBuilder(Thread.currentThread().getName());
      buff.append(" ");
      buff.append(message);
      message = buff.toString();
      if (Level.ERROR.toString().equalsIgnoreCase(logLevel.toString())) {
        logErrorMessage(throwable, message);
      } else if (Level.DEBUG.toString().equalsIgnoreCase(logLevel.toString())) {
        logDebugMessage(throwable, message);
      } else if (Level.INFO.toString().equalsIgnoreCase(logLevel.toString())) {
        logInfoMessage(throwable, message);
      } else if (Level.WARN.toString().equalsIgnoreCase(logLevel.toString())) {
        logWarnMessage(throwable, message);
      } else if (Level.AUDIT.toString().equalsIgnoreCase(logLevel.toString())) {
        audit(message);
      } else if (Level.STATISTICS == logLevel) {
        statistic(message);
      }

    } catch (Throwable t) {
      logger.error(t);
    }
  }

  private void logErrorMessage(Throwable throwable, String message) {

    if (null == throwable) {
      logger.error(message);
    } else {
      logger.error(message, throwable);
    }
  }

  private void logInfoMessage(Throwable throwable, String message) {

    if (null == throwable) {
      logger.info(message);
    } else {
      logger.info(message, throwable);
    }
  }

  private void logDebugMessage(Throwable throwable, String message) {

    if (null == throwable) {
      logger.debug(message);
    } else {
      logger.debug(message, throwable);
    }
  }

  private void logWarnMessage(Throwable throwable, String message) {

    if (null == throwable) {
      logger.warn(message);
    } else {
      logger.warn(message, throwable);
    }
  }

  public boolean isInfoEnabled() {
    return logger.isInfoEnabled();
  }

  public void warn(String message) {
    if (isWarnEnabled()) {
      logMessage(Level.WARN, null, message);
    }
  }

  public void setEventProperties(String propertyName, String propertyValue) {
    MDC.put(propertyName, propertyValue);
  }

  /**
   * log audit log
   *
   * @param msg audit log message
   */
  @Override public void audit(String msg) {
    String hostName;
    String username;
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
    String threadid = Thread.currentThread().getId() + "";
    logger.log(AuditLevel.AUDIT,
        "[" + hostName + "]" + "[" + username + "]" + "[Thread-" + threadid + "]" + msg);
  }

  @Override public void statistic(String message) {
    logger.log(StatisticLevel.STATISTIC, message);
  }

  /**
   * Specifies the logging level.
   */
  enum Level {

    NONE(0),
    DEBUG(1),
    INFO(2),
    STATISTICS(3),
    ERROR(4),
    AUDIT(5),
    WARN(6);

    /**
     * Constructor.
     *
     * @param level
     */
    Level(final int level) {
    }
  }
}
