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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class TestCarbonInterpreter {

  static HttpServer server = null;

  @BeforeClass
  public static void setup() throws Exception {
    server = HttpServer.create(new InetSocketAddress("localhost", 8123), 0);
    server.createContext("/table/sql", new FakePostHandler());
    server.start();
  }

  @AfterClass
  public static void tearDown() {
    Optional.of(server).ifPresent(serverSocket -> serverSocket.stop(0));
  }

  @Test(timeout = 5000)
  public void testInterpreterSelectSuccessResponse() throws InterpreterException {
    Properties properties = new Properties();
    properties.put(CarbonInterpreter.CARBON_QUERY_API_URL, "http://localhost:8123/table/sql");
    CarbonInterpreter interpreter = new CarbonInterpreter(properties);
    InterpreterResult result = interpreter.interpret("show tables", null);
    String expectedFormattedResult = "{\"code\":\"SUCCESS\",\"msg\":[{\"type\":\"TABLE\",\"data\"" +
            ":\"database\\ttableName\\tisTemporary\\ndefault\\tsinka6\\tfalse\\ndefault\\tsinka7\\tfalse\"}]}";
    assertEquals(expectedFormattedResult, result.toJson());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals(InterpreterResult.Type.TABLE, result.message().get(0).getType());
  }
}

class FakePostHandler implements HttpHandler {

  @Override
  public void handle(HttpExchange he) throws IOException {
    InputStreamReader isr = new InputStreamReader(he.getRequestBody(), "utf-8");
    BufferedReader br = new BufferedReader(isr);
    String query = br.readLine();
    String response = "";
    if (query.equals("{\"sqlStatement\":\"show tables\"}")) {
      response = "{\n" +
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
    }
    he.sendResponseHeaders(200, response.length());
    OutputStream os = he.getResponseBody();
    os.write(response.toString().getBytes());
    os.close();
  }
}
