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

import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;

import org.apache.hadoop.conf.Configuration;

public class CarbonTestUtil {

  public static Configuration configuration = new Configuration();

  public static ValueEncoderMeta createValueEncoderMeta() {
    ColumnarFormatVersion version =
        CarbonProperties.getInstance().getFormatVersion();

    switch (version) {
      case V1:
      case V2:
        return new ValueEncoderMeta();
      case V3:
        return new ColumnPageEncoderMeta();
      default:
        throw new UnsupportedOperationException("unsupported version: " + version);
    }
  }
}
