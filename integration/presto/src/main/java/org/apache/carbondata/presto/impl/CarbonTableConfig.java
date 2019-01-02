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

package org.apache.carbondata.presto.impl;

import io.airlift.configuration.Config;

/**
 * Configuration read from etc/catalog/carbondata.properties
 */
public class CarbonTableConfig {

  //read from config
  private String unsafeMemoryInMb;
  private String enableUnsafeInQueryExecution;
  private String enableUnsafeColumnPage;
  private String enableUnsafeSort;
  private String enableQueryStatistics;
  private String batchSize;
  private String s3A_acesssKey;
  private String s3A_secretKey;
  private String s3_acesssKey;
  private String s3_secretKey;
  private String s3N_acesssKey;
  private String s3N_secretKey;
  private String endPoint;
  private String pushRowFilter;


  public String getUnsafeMemoryInMb() {
    return unsafeMemoryInMb;
  }

  @Config("carbon.unsafe.working.memory.in.mb")
  public CarbonTableConfig setUnsafeMemoryInMb(String unsafeMemoryInMb) {
    this.unsafeMemoryInMb = unsafeMemoryInMb;
    return this;
  }

  public String getEnableUnsafeInQueryExecution() {
    return enableUnsafeInQueryExecution;
  }

  @Config("enable.unsafe.in.query.processing")
  public CarbonTableConfig setEnableUnsafeInQueryExecution(String enableUnsafeInQueryExecution) {
    this.enableUnsafeInQueryExecution = enableUnsafeInQueryExecution;
    return this;
  }

  public String getEnableUnsafeColumnPage() { return enableUnsafeColumnPage; }

  @Config("enable.unsafe.columnpage")
  public CarbonTableConfig setEnableUnsafeColumnPage(String enableUnsafeColumnPage) {
    this.enableUnsafeColumnPage = enableUnsafeColumnPage;
    return this;
  }

  public String getEnableUnsafeSort() { return enableUnsafeSort; }

  @Config("enable.unsafe.sort")
  public CarbonTableConfig setEnableUnsafeSort(String enableUnsafeSort) {
    this.enableUnsafeSort = enableUnsafeSort;
    return this;
  }

  public String getEnableQueryStatistics() { return enableQueryStatistics; }

  @Config("enable.query.statistics")
  public CarbonTableConfig setEnableQueryStatistics(String enableQueryStatistics) {
    this.enableQueryStatistics = enableQueryStatistics;
    return this;
  }

  public String getBatchSize() { return batchSize; }

  @Config("query.vector.batchSize")
  public CarbonTableConfig setBatchSize(String batchSize) {
    this.batchSize = batchSize;
    return this;
  }

  public String getS3A_AcesssKey() {
    return s3A_acesssKey;
  }

  public String getS3A_SecretKey() {
    return s3A_secretKey;
  }

  public String getS3_AcesssKey() {
    return s3_acesssKey;
  }

  public String getS3_SecretKey() {
    return s3_secretKey;
  }

  public String getS3N_AcesssKey() {
    return s3N_acesssKey;
  }

  public String getS3N_SecretKey() {
    return s3N_secretKey;
  }

  public String getS3EndPoint() {
    return endPoint;
  }


  @Config("fs.s3a.access.key")
  public CarbonTableConfig setS3A_AcesssKey(String s3A_acesssKey) {
    this.s3A_acesssKey = s3A_acesssKey;
    return this;
  }

  @Config("fs.s3a.secret.key")
  public CarbonTableConfig setS3A_SecretKey(String s3A_secretKey) {
    this.s3A_secretKey = s3A_secretKey;
    return this;
  }

  @Config("fs.s3.awsAccessKeyId")
  public CarbonTableConfig setS3_AcesssKey(String s3_acesssKey) {
    this.s3_acesssKey = s3_acesssKey;
    return this;
  }

  @Config("fs.s3.awsSecretAccessKey")
  public CarbonTableConfig setS3_SecretKey(String s3_secretKey) {
    this.s3_secretKey = s3_secretKey;
    return this;
  }
  @Config("fs.s3n.awsAccessKeyId")
  public CarbonTableConfig setS3N_AcesssKey(String s3N_acesssKey) {
    this.s3N_acesssKey = s3N_acesssKey;
    return this;
  }

  @Config("fs.s3.awsSecretAccessKey")
  public CarbonTableConfig setS3N_SecretKey(String s3N_secretKey) {
    this.s3N_secretKey = s3N_secretKey;
    return this;
  }
  @Config("fs.s3a.endpoint")
  public CarbonTableConfig setS3EndPoint(String endPoint) {
    this.endPoint = endPoint;
    return this;
  }

  public String getPushRowFilter() {
    return pushRowFilter;
  }

  @Config("carbon.push.rowfilters.for.vector")
  public void setPushRowFilter(String pushRowFilter) {
    this.pushRowFilter = pushRowFilter;
  }
}
