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
import org.apache.hadoop.fs.Path;

public class CarbonConfiguration implements Serializable {

  private static final long serialVersionUID = 3811544264223154007L;
  private transient Configuration configuration;
  private HashMap<String, String> extraConf;

  public CarbonConfiguration(Map<String, String> settings) {
    this.extraConf = new HashMap<>();
    extraConf.putAll(settings);
  }

  public CarbonConfiguration(Configuration configuration) {
    this.configuration = configuration;
    HashMap<String, String> s3Conf = new HashMap<String, String>();
    s3Conf.put("fs.s3a.access.key", configuration.get("fs.s3a.access.key"));
    s3Conf.put("fs.s3a.secret.key", configuration.get("fs.s3a.secret.key"));
    this.extraConf = s3Conf;
  }


  public CarbonConfiguration() {
    this.configuration = new Configuration();
    configuration.addResource(new Path("../core-default.xml"));
    HashMap<String, String> s3Conf = new HashMap<String, String>();
    s3Conf.put("fs.s3a.access.key", configuration.get("fs.s3a.access.key"));
    s3Conf.put("fs.s3a.secret.key", configuration.get("fs.s3a.secret.key"));
    this.extraConf = s3Conf;
  }

  public Configuration getConfiguration() {
    if (configuration == null) {
      this.configuration = new Configuration();
      configuration.addResource(new Path("../core-default.xml"));
      if (extraConf != null && !extraConf.isEmpty()) {
        for (Map.Entry<String, String> entry : extraConf.entrySet()) {
          if (entry.getValue() != null) {
            configuration.set(entry.getKey(), entry.getValue());
          }
        }
      }
    }
    return configuration;
  }

//  public static HashMap<String, String> getS3Configuration(Configuration conf) {
//    HashMap<String, String> s3Conf = new HashMap<String, String>();
//    s3Conf.put("fs.s3a.access.key", conf.get("fs.s3a.access.key"));
//    s3Conf.put("fs.s3a.secret.key", conf.get("fs.s3a.secret.key"));
//    return s3Conf;
//  }

}
