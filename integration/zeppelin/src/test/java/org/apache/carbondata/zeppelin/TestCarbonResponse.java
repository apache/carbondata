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

package org.apache.carbondata.zeppelin;

import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.carbondata.zeppelin.response.CarbonResponse;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestCarbonResponse {

  @Test(expected = JsonMappingException.class)
  public void testBodyIsEmpty() throws IOException {
    String input = "";
    CarbonResponse.parse(new ByteArrayInputStream(input.getBytes()));
  }

  @Test
  public void testSuccessResponse() throws IOException {
    String input = "{\n" +
            "    \"responseId\": 19435528129427470,\n" +
            "    \"message\": \"SUCCESS\",\n" +
            "    \"rows\": [\n" +
            "        [\n" +
            "            \"database\",\n" +
            "            \"tableName\",\n" +
            "            \"isTemporary\"\n" +
            "        ],\n" +
            "        [\n" +
            "            \"default\",\n" +
            "            \"sinka6\",\n" +
            "            false\n" +
            "        ],\n" +
            "        [\n" +
            "            \"default\",\n" +
            "            \"sinka7\",\n" +
            "            false\n" +
            "        ]\n" +
            "    ]\n" +
            "}";
    Object[][] expectedResponse = new Object[3][];
    expectedResponse[0] = new Object[]{"database", "tableName", "isTemporary"};
    expectedResponse[1] = new Object[]{"default", "sinka6", false};
    expectedResponse[2] = new Object[]{"default", "sinka7", false};
    CarbonResponse successResponse = CarbonResponse.parse(new ByteArrayInputStream(input.getBytes())).get();
    assertEquals("SUCCESS", successResponse.getMessage());
    assertEquals("19435528129427470", successResponse.getResponseId());
    assertTrue(Arrays.deepEquals(expectedResponse, successResponse.getRows()));
    assertEquals(input, successResponse.getFullResponse());
  }

  @Test
  public void testErrorResponse() throws IOException {
    String input = "{\n" +
            "    \"timestamp\": 1531884083849,\n" +
            "    \"status\": 500,\n" +
            "    \"error\": \"Internal Server Error\",\n" +
            "    \"exception\": \"org.apache.carbondata.store.api.exception.StoreException\",\n" +
            "    \"message\": \"org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException: " +
            "Table or view 'sinka6' already exists in database 'default';\",\n" +
            "    \"path\": \"/table/sql\"\n" +
            "}";
    CarbonResponse errorResponse = CarbonResponse.parse(new ByteArrayInputStream(input.getBytes())).get();
    assertEquals("org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException: " +
            "Table or view 'sinka6' already exists in database 'default';", errorResponse.getMessage());
    assertEquals("org.apache.carbondata.store.api.exception.StoreException", errorResponse.getException());
    assertEquals(1531884083849L, errorResponse.getTimestamp());
    assertEquals("Internal Server Error", errorResponse.getError());
    assertEquals(500, errorResponse.getStatus());
    assertEquals("/table/sql", errorResponse.getPath());
    assertEquals(input, errorResponse.getFullResponse());
  }
}
