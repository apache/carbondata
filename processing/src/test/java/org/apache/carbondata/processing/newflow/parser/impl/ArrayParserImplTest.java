/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.processing.newflow.parser.impl;

import org.apache.carbondata.processing.newflow.complexobjects.ArrayObject;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertNotNull;

public class ArrayParserImplTest {

  private static String delimiter = null;
  private static String nullFormat = null;
  private static ArrayParserImpl arrayParserImpl = null;
  private static StructParserImpl structParserImpl = null;

  @BeforeClass public static void setUp() {
    delimiter = "!";
    nullFormat = ";";
    arrayParserImpl = new ArrayParserImpl(delimiter, nullFormat);
    structParserImpl = new StructParserImpl(delimiter, nullFormat);
  }

  @AfterClass public static void tearDown() {
    delimiter = null;
    nullFormat = null;
  }

  @Test public void testArrayParser() {
    arrayParserImpl.addChildren(structParserImpl);
    String[] data = { "This", "string", "is", "used", "for", "Testing" };
    ArrayObject array = arrayParserImpl.parse(data);
    assertNotNull(array);
  }

}

