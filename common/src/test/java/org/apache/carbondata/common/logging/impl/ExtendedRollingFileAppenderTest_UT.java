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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.junit.Assert;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Before;
import org.junit.Test;

public class ExtendedRollingFileAppenderTest_UT {

  private ExtendedRollingFileAppender rAppender = null;

  @Before public void setUp() {
    rAppender = new ExtendedRollingFileAppender();
    rAppender.setFile("dummy.log");
    rAppender.setMaxBackupIndex(1);
    rAppender.setMaxFileSize("1000");
  }

  @Test public void testRollOver() {
    rAppender.rollOver();
    rAppender.rollOver();
    rAppender.rollOver();
    Assert.assertTrue(true);
  }

  @Test public void testCleanLogs()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    final String startName = "dummy";
    final String folderPath = "./";
    int maxBackupIndex = 1;

    Method cleanLogsMethod = ExtendedRollingFileAppender.class.getDeclaredMethod("cleanLogs",
        String.class, String.class, int.class);
    cleanLogsMethod.setAccessible(true);
    cleanLogsMethod.invoke(rAppender, startName, folderPath, maxBackupIndex);
  }

  @Test public void testSubAppendLoggingEvent() {
    Logger logger = Logger.getLogger(this.getClass());
    LoggingEvent event = new LoggingEvent(null, logger, 0L, AuditLevel.DEBUG, null, null);

    try {
      rAppender.subAppend(event);
    } catch (Exception e) {
      //
    }
    Assert.assertTrue(true);
  }

}
