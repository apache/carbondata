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
package org.apache.carbondata.core.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * This is a serializable wrapper class for configuration that have to be passed from driver to
 * executor.
 */
public class CarbonConfiguration implements Serializable {

  private static final long serialVersionUID = 3811544264223154007L;

  private HashMap<String, String> extraConf;

  private transient Configuration configuration;

  public CarbonConfiguration(Configuration configuration) {
    HashMap<String, String> s3Conf = new HashMap<String, String>();
    s3Conf.put("fs.s3a.access.key", configuration.get("fs.s3a.access.key", ""));
    s3Conf.put("fs.s3a.secret.key", configuration.get("fs.s3a.secret.key", ""));
    s3Conf.put("fs.s3a.endpoint", configuration.get("fs.s3a.endpoint", ""));
    this.extraConf = s3Conf;
    this.configuration = configuration;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public Map<String, String> getExtraConf() {
    return extraConf;
  }

  public Configuration fillExtraConfigurations(Configuration configuration) {
    for (Map.Entry<String, String> entry : extraConf.entrySet()) {
      configuration.set(entry.getKey(), entry.getValue());
    }
    return configuration;
  }

}