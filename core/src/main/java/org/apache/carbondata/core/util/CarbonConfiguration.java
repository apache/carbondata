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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.carbondata.core.datastore.compression.CompressorFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class CarbonConfiguration implements Serializable {

  private static final long serialVersionUID = 3811544264223154007L;
  private transient Configuration configuration;
  private byte[] confBytes;

  public CarbonConfiguration(Configuration configuration) {
    initialize(configuration);
  }

  public CarbonConfiguration() {
    this.configuration = new Configuration();
    configuration.addResource(new Path("../core-default.xml"));
    initialize(configuration);
  }

  private void initialize(Configuration configuration) {
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    try {
      ObjectOutputStream oos = new ObjectOutputStream(bao);
      configuration.write(oos);
      oos.close();
      this.confBytes =
          CompressorFactory.getInstance().getCompressor().compressByte(bao.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Configuration getConfiguration() {
    if (configuration == null) {
      if (confBytes == null) {
        throw new RuntimeException("Configuration not specified");
      }
      configuration = new Configuration(false);
      configuration.addResource(new Path("../core-default.xml"));
      ByteArrayInputStream bias = new ByteArrayInputStream(
          CompressorFactory.getInstance().getCompressor().unCompressByte(confBytes));
      try {
        ObjectInputStream ois = new ObjectInputStream(bias);
        configuration.readFields(ois);
        ois.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return configuration;
  }
}
