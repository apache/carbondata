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

package org.apache.carbondata.hive;

import java.util.Map;

import org.apache.carbondata.core.datastore.impl.FileFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.StorageHandlerInfo;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;

/**
 * *NOTE*: Do not Remove this class as this is the storage handler for carbondata for HIVE.
 */
public class CarbonStorageHandler implements HiveStorageHandler {

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return MapredCarbonInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return MapredCarbonOutputFormat.class;
  }

  @Override
  public Class<? extends AbstractSerDe> getSerDeClass() {
    return CarbonHiveSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return null;
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
    return null;
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

  }

  @Override
  public void configureInputJobCredentials(TableDesc tableDesc, Map<String, String> map) {

  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {

  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {

  }

  @Override
  public StorageHandlerInfo getStorageHandlerInfo(Table table) throws MetaException {
    return null;
  }

  @Override
  public LockType getLockType(WriteEntity writeEntity) {
    return null;
  }

  @Override
  public void setConf(Configuration configuration) {

  }

  @Override
  public Configuration getConf() {
    return FileFactory.getConfiguration();
  }
}
