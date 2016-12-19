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
package org.apache.carbondata.processing.newflow.dictionary;

import org.apache.carbondata.core.cache.dictionary.ColumnDictionaryInfo;
import org.apache.carbondata.core.cache.dictionary.ForwardDictionary;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.devapi.DictionaryGenerationException;
import org.apache.carbondata.core.keygenerator.directdictionary.timestamp.TimeStampDirectDictionaryGenerator;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DirectDictionaryTest {
  private static DirectDictionary directDictionary;
  private static ColumnDictionaryInfo columnDictionaryInfo;
  private static ForwardDictionary forwardDictionary;

  @BeforeClass public static void setUp() {
    columnDictionaryInfo = new ColumnDictionaryInfo(DataType.INT);
    forwardDictionary = new ForwardDictionary(columnDictionaryInfo);
    TimeStampDirectDictionaryGenerator timeStampDirectDictionaryGenerator =
        new TimeStampDirectDictionaryGenerator("01/01/2000 02:02:02");

    directDictionary = new DirectDictionary(timeStampDirectDictionaryGenerator);
  }

  @Test public void testGetOrGenerateKey() throws DictionaryGenerationException {
    int expectedValue = 1;
    int actualValue = directDictionary.getOrGenerateKey("01/01/2000 02:02:02");
    assertEquals(expectedValue, actualValue);
  }
}
