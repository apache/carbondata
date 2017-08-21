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

package org.apache.carbondata.common.logging.ft;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;

import junit.framework.TestCase;
import org.apache.log4j.LogManager;
import org.apache.log4j.MDC;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LoggingServiceTest_FT extends TestCase {

  private static LogService logger =
      LogServiceFactory.getLogService(LoggingServiceTest_FT.class.getName());

  @Before public void setUp() throws Exception {
    MDC.put("MODULE", "Function Test");
    MDC.put("USER_NAME", "testuser");
    MDC.put("CLIENT_IP", "127.0.0.1");
    MDC.put("OPERATRION", "log");
  }

  @Test public void testIsAuditFileCreated() {
    File f = new File("./unibiaudit.log");
    Assert.assertFalse(f.exists());
  }

  @Test public void testAudit() {

    String expectedAuditLine =
        "[main] AUDIT [org.apache.carbondata.common.logging.ft.LoggingServiceTest_FT] 127.0.0.1 "
            + "testuser Function Test log- audit message created";
    logger.audit("audit message created");

    LogManager.shutdown();

    try {
      FileInputStream fstream = new FileInputStream("./carbondataaudit.log");
      BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
      String actualAuditLine = null;
      String strLine = null;
      while ((strLine = br.readLine()) != null) {
        actualAuditLine = strLine;
      }

      System.out.println(actualAuditLine);

      if (actualAuditLine != null) {
        int index = actualAuditLine.indexOf("[main]");
        actualAuditLine = actualAuditLine.substring(index);
        Assert.assertEquals(expectedAuditLine, actualAuditLine);
      } else {
        Assert.assertTrue(false);
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      Assert.assertTrue(true);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

  }
}
