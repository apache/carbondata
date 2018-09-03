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

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.carbondata.zeppelin.response.CarbonResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Carbon based interpreter for zeppelin
 */
public class CarbonInterpreter extends Interpreter {

  public static final Logger logger = LoggerFactory.getLogger(CarbonInterpreter.class);

  static final char TAB = '\t';

  static final String LF = "\n";

  /**
   * Property which can be set in zeppelin to carbon REST API server
   */
  public static final String CARBON_QUERY_API_URL = "carbon.query.api.url";

  /**
   * These are the queries which need Table like output format
   */
  private static final String[] SEARCH_QUERIES = {"select", "list", "show", "desc", "explain"};

  public CarbonInterpreter(Properties properties) {
    super(properties);
  }

  @Override
  public void open() throws InterpreterException {
  }

  @Override
  public void close() throws InterpreterException {
  }

  @Override
  public void cancel(InterpreterContext interpreterContext) throws InterpreterException {
  }

  @Override
  public int getProgress(InterpreterContext interpreterContext) throws InterpreterException {
    return 0;
  }

  @Override
  public FormType getFormType() throws InterpreterException {
    return FormType.SIMPLE;
  }

  @Override
  public InterpreterResult interpret(String sql, InterpreterContext interpreterContext)
          throws InterpreterException {
    try {
      return executeQuery.apply(sql);
    } catch (RuntimeException e) {
      logger.error("failed to query data in carbon ", e);
      return new InterpreterResult(InterpreterResult.Code.ERROR, e.getMessage());
    }
  }

  /**
   * This will execute the given sql Query by sending a post request on CARBON_QUERY_API_URL
   */
  private Function<String, HttpResponse> doPost = sql -> {
    // prepare the post body
    String postContent = new StringBuilder("{\"sqlStatement\":")
            .append("\"").append(sql).append("\"" + "}").toString();
    logger.debug("post:" + postContent);

    // prepare entity and set content type
    StringEntity entity = new StringEntity(postContent, "UTF-8");
    entity.setContentType("application/json; charset=UTF-8");

    // get the POST url from interpreter property
    String postURL = getProperty(CARBON_QUERY_API_URL);
    logger.debug("post url:" + postURL);

    // do POST and get response
    HttpPost postRequest = new HttpPost(postURL);
    postRequest.setEntity(entity);
    HttpClient httpClient = HttpClientBuilder.create().build();
    try {
      return httpClient.execute(postRequest);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  };

  /**
   * Check if output has to be sent as a able to zeppelin
   */
  private Function<String, Boolean> isTableFormatOutput = sql ->
          (StringUtils.startsWithAny(sql, SEARCH_QUERIES));

  /**
   * returns InterpreterResult from CarbonResponse
   */
  private BiFunction<String, CarbonResponse, InterpreterResult> getResult = (sql, response) -> {
    if (isTableFormatOutput.apply(sql.toLowerCase().trim())) {
      //format only select queries and return as table
      String formattedResult = Arrays
              .stream(response.getRows())
              .filter(Objects::nonNull)
              .map(row -> StringUtils.join(row, TAB))
              .collect(Collectors.joining(LF));
      return new InterpreterResult(InterpreterResult.Code.SUCCESS, InterpreterResult.Type.TABLE,
              formattedResult);
    } else {
      return new InterpreterResult(InterpreterResult.Code.SUCCESS, InterpreterResult.Type.TEXT,
              response.getMessage());
    }
  };

  /**
   * Executes the given sql and return formatted result
   */
  private Function<String, InterpreterResult> executeQuery = sql -> {
    try {
      HttpResponse response = doPost.apply(sql);
      // always close the content after reading fully to release connection
      // IOUtils.toString will completely read the content
      try (InputStream content = response.getEntity().getContent()) {
        Optional<CarbonResponse> carbonResponse = CarbonResponse.parse(content);
        int code = response.getStatusLine().getStatusCode();
        if (code != 200) {
          StringBuilder errorMessage = new StringBuilder("Failed : HTTP error code " + code + " .");
          carbonResponse.ifPresent(rsp -> {
            logger.error("Failed to execute query: " + rsp.getFullResponse());
            errorMessage.append(rsp.getMessage());
          });
          return new InterpreterResult(InterpreterResult.Code.ERROR, errorMessage.toString());
        } else {
          return carbonResponse.map(rsp -> getResult.apply(sql, rsp))
                  .orElseGet(() -> new InterpreterResult(InterpreterResult.Code.SUCCESS,
                          InterpreterResult.Type.TEXT,
                          "Query Success, but unable to parse response"));
        }
      }
    } catch (IOException e) {
      logger.error("Error executing query ", e);
      return new InterpreterResult(InterpreterResult.Code.ERROR, e.getMessage());
    }
  };
}
