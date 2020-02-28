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

package org.apache.carbondata.processing.util;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.impl.AuditLevel;
import org.apache.carbondata.core.util.CarbonProperties;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

/**
 * Audit logger.
 * User can configure log4j to log to a separate file. For example
 *
 *  log4j.logger.carbon.audit=DEBUG, audit
 *  log4j.appender.audit=org.apache.log4j.FileAppender
 *  log4j.appender.audit.File=/opt/logs/audit.out
 *  log4j.appender.audit.Threshold=AUDIT
 *  log4j.appender.audit.Append=false
 *  log4j.appender.audit.layout=org.apache.log4j.PatternLayout
 *  log4j.appender.audit.layout.ConversionPattern=%m%n
 */
@InterfaceAudience.Internal
public class Auditor {
  private static final Logger LOGGER = Logger.getLogger("carbon.audit");
  private static final Gson gson = new GsonBuilder().disableHtmlEscaping().create();
  private static String username;

  static {
    try {
      username = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      username = "unknown";
    }
  }

  /**
   * call this method to record audit log when operation is triggered
   * @param opName operation name
   * @param opId operation unique id
   */
  public static void logOperationStart(String opName, String opId) {
    if (CarbonProperties.isAuditEnabled()) {
      Objects.requireNonNull(opName);
      Objects.requireNonNull(opId);
      OpStartMessage message = new OpStartMessage(opName, opId);
      Gson gson = new GsonBuilder().disableHtmlEscaping().create();
      String json = gson.toJson(message);
      LOGGER.log(AuditLevel.AUDIT, json);
    }
  }

  /**
   * call this method to record audit log when operation finished
   * @param opName operation name
   * @param opId operation unique id
   * @param success true if operation success
   * @param table carbon dbName and tableName
   * @param opTime elapse time in Ms for this operation
   * @param extraInfo extra information to include in the audit log
   */
  public static void logOperationEnd(String opName, String opId, boolean success, String table,
      String opTime, Map<String, String> extraInfo) {
    if (CarbonProperties.isAuditEnabled()) {
      Objects.requireNonNull(opName);
      Objects.requireNonNull(opId);
      Objects.requireNonNull(opTime);
      OpEndMessage message = new OpEndMessage(opName, opId, table, opTime,
          success ? OpStatus.SUCCESS : OpStatus.FAILED,
          extraInfo != null ? extraInfo : new HashMap<String, String>());
      String json = gson.toJson(message);
      LOGGER.log(AuditLevel.AUDIT, json);
    }
  }

  private enum OpStatus {
    // operation started
    START,

    // operation succeed
    SUCCESS,

    // operation failed
    FAILED
  }

  // log message for operation start, it is written as a JSON record in the audit log
  private static class OpStartMessage {
    private String time;
    private String username;
    private String opName;
    private String opId;
    private OpStatus opStatus;

    OpStartMessage(String opName, String opId) {
      FastDateFormat format =
          FastDateFormat.getDateTimeInstance(FastDateFormat.LONG, FastDateFormat.LONG);
      this.time = format.format(new Date());
      this.username = Auditor.username;
      this.opName = opName;
      this.opId = opId;
      this.opStatus = OpStatus.START;
    }

    // No one actually invoke this, it is only for findbugs
    public String getTime() {
      return time;
    }

    // No one actually invoke this, it is only for findbugs
    public String getUsername() {
      return username;
    }

    // No one actually invoke this, it is only for findbugs
    public String getOpName() {
      return opName;
    }

    // No one actually invoke this, it is only for findbugs
    public String getOpId() {
      return opId;
    }

    // No one actually invoke this, it is only for findbugs
    public OpStatus getOpStatus() {
      return opStatus;
    }
  }

  // log message for operation end, it is written as a JSON record in the audit log
  private static class OpEndMessage {
    private String time;
    private String username;
    private String opName;
    private String opId;
    private OpStatus opStatus;
    private String opTime;
    private String table;
    private Map<String, String> extraInfo;

    OpEndMessage(String opName, String opId, String table, String opTime,
        OpStatus opStatus, Map<String, String> extraInfo) {
      FastDateFormat format =
          FastDateFormat.getDateTimeInstance(FastDateFormat.LONG, FastDateFormat.LONG);
      this.time = format.format(new Date());
      this.username = Auditor.username;
      this.opName = opName;
      if (table != null) {
        this.table = table;
      } else {
        this.table = "NA";
      }
      this.opTime = opTime;
      this.opId = opId;
      this.opStatus = opStatus;
      this.extraInfo = extraInfo;
    }

    // No one actually invoke this, it is only for findbugs
    public String getTime() {
      return time;
    }

    // No one actually invoke this, it is only for findbugs
    public String getUsername() {
      return username;
    }

    // No one actually invoke this, it is only for findbugs
    public String getOpName() {
      return opName;
    }

    // No one actually invoke this, it is only for findbugs
    public String getOpId() {
      return opId;
    }

    // No one actually invoke this, it is only for findbugs
    public OpStatus getOpStatus() {
      return opStatus;
    }

    // No one actually invoke this, it is only for findbugs
    public String getTable() {
      return table;
    }

    // No one actually invoke this, it is only for findbugs
    public String getOpTime() {
      return opTime;
    }

    // No one actually invoke this, it is only for findbugs
    public Map<String, String> getExtraInfo() {
      return extraInfo;
    }
  }

}
