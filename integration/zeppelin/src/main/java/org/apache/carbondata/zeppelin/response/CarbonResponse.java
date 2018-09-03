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

package org.apache.carbondata.zeppelin.response;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.io.IOUtils;



/**
 * acts as a response object from carbon horizon server
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CarbonResponse {

  private static final ObjectMapper mapper = new ObjectMapper();

  private String responseId;

  private String message;

  private Object[][] rows;

  long timestamp;

  int status;

  String error;

  String exception;

  String path;

  String fullResponse;


  public String getResponseId() {
    return responseId;
  }

  public void setResponseId(String responseId) {
    this.responseId = responseId;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public Object[][] getRows() {
    return rows;
  }

  public void setRows(Object[][] rows) {
    this.rows = rows;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public int getStatus() {
    return status;
  }

  public void setStatus(int status) {
    this.status = status;
  }

  public String getError() {
    return error;
  }

  public void setError(String error) {
    this.error = error;
  }

  public String getException() {
    return exception;
  }

  public void setException(String exception) {
    this.exception = exception;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getFullResponse() {
    return fullResponse;
  }

  public void setFullResponse(String fullResponse) {
    this.fullResponse = fullResponse;
  }

  /**
   * Reads the input stream for JSON and return a CarbonResponse instance
   * PS: Caller responsible for closing the stream
   *
   * @param inputStream
   * @return
   * @throws IOException
   */
  public static Optional<CarbonResponse> parse(InputStream inputStream) throws IOException {
    String plainTextResponse = IOUtils.toString(inputStream, "UTF-8");
    CarbonResponse response = mapper.readValue(plainTextResponse, CarbonResponse.class);
    response.setFullResponse(plainTextResponse);
    return Optional.of(response);
  }
}