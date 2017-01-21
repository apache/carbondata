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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FileUtilTest_UT extends TestCase {

  /**
   * @throws Exception
   */
  @Before public void setUp() throws Exception {
    File f = new File("myfile.txt");
    if (!f.exists()) {
      f.createNewFile();
    }
  }

  /**
   * @throws Exception
   */
  @After public void tearDown() throws Exception {
    File f = new File("myfile.txt");
    if (f.exists()) {
      f.delete();
    }
  }

  @Test public void testClose() {
    try {
      FileInputStream in = new FileInputStream(new File("myfile.txt"));
      FileUtil.close(in);
      assertTrue(true);
    } catch (FileNotFoundException e) {
      assertTrue(false);
    }
  }
}
